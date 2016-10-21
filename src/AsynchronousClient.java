import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/*
 * This class gets the requests from the write queue and processes them 
 * asynchronously. Since, the write requests may have to be replicated 
 * to secondary servers, it fetches the response from all the corresponding 
 * servers and checks for errors (if any). The response is then sent 
 * back through the send method from the Manager1 instance to the 
 * primary server.
 */

public class AsynchronousClient implements Runnable {
	private InetAddress hostAddress;
	private int port;
	//This is the local queue where we store the DataPackets whose requests haven't
	//completed
	private ArrayBlockingQueue<DataPacket> setQueue;
	private DataPacket packet;
	private ArrayList<DataPacket> checkPackets= new ArrayList<DataPacket>();
	private SocketChannel socketChannel;
	private int numReplications;
	
	//This HashMap maps each socketChannel to the number of requests it is waiting for inside the local queue
	private HashMap<SocketChannel, Integer> requestCount = new HashMap<SocketChannel, Integer>();
	
	//This HashMap monitors the number of responses received by each DataPacket, once this number 
	//equals the numReplications, the response is sent by by the socket of the primary server
	private HashMap<DataPacket, Integer> replicationCounter = new HashMap<DataPacket, Integer>();
	
	//This HashMap maps each IP:PORT tuple to its corresponding socket
	private HashMap<String, SocketChannel> AddresstoSocket = new HashMap<String, SocketChannel>();
	private ByteBuffer readBuffer = ByteBuffer.allocate(2048);
	private Selector selector;
	//This byte array stores the correct response
	private byte[] storedmessage;
	//This byte array stores the incorrect response
	private byte[] errormessage;

	public AsynchronousClient(String hostAddress, int numReplications, ArrayBlockingQueue<DataPacket> setQueue, List<String> mcAddresses) throws IOException {
		this.setQueue = setQueue;
		this.hostAddress = InetAddress.getByName(hostAddress.split(":")[0]);
		this.port = Integer.parseInt(hostAddress.split(":")[1]);
		// Create a non-blocking socket channel
		this.socketChannel = SocketChannel.open();
		this.socketChannel.configureBlocking(false);
		this.selector = this.initSelector();
		this.numReplications = numReplications;
		requestCount.put(this.socketChannel, 0);
		this.socketChannel.register(this.selector, SelectionKey.OP_CONNECT);
		AddresstoSocket.put(hostAddress,this.socketChannel);
		if(numReplications > 1)
		{
			for(String node: mcAddresses)
			{
				if(!node.equals(hostAddress))
				{	
					InetAddress address = InetAddress.getByName(node.split(":")[0]);
					int port = Integer.parseInt(node.split(":")[1]);
					SocketChannel tmpinitSocket = this.initiateConnection(address, port);
					AddresstoSocket.put(node, tmpinitSocket);
					requestCount.put(tmpinitSocket, 0);
				}
			}
		}
		// Kick off connection establishment
		this.socketChannel.connect(new InetSocketAddress(this.hostAddress, this.port));
	}

	@Override
	public void run() {
		while (true) {
			try {	
				//Since the take method blocks the Queue until a value is available, we 
				//use poll instead for asynchronous behaviour
				//get the DataPacket instance from the queue
				packet = this.setQueue.poll();
				if(packet!=null)
				{
					checkPackets.add(packet);
					//Stop the time for Tqueue as the request is dequeued
					packet.Tqueue = System.nanoTime() - packet.Tqueue;
					replicationCounter.put(packet, 0);
					//Start the time for Tserver as the request is processed by the server
					packet.Tserver = System.nanoTime();
					this.socketChannel.write(ByteBuffer.wrap(packet.data));
					//register an interest in reading on this channel
					this.socketChannel.keyFor(this.selector).interestOps(SelectionKey.OP_READ);
					if(this.numReplications > 1)
					{
						for(int i = 1; i < this.numReplications; i++)
						{
							SocketChannel tmpsocket = AddresstoSocket.get(packet.replicaServers.get(i));
							tmpsocket.write(ByteBuffer.wrap(packet.data));
							tmpsocket.keyFor(this.selector).interestOps(SelectionKey.OP_READ);
						}
					}
				}

				// Wait for an event one of the registered channels
				this.selector.select();

				// Iterate over the set of keys for which events are available
				Iterator selectedKeys = this.selector.selectedKeys().iterator();
				while (selectedKeys.hasNext()) {
					SelectionKey key = (SelectionKey) selectedKeys.next();
					selectedKeys.remove();

					if (!key.isValid()) {
						continue;
					}

					// Check what event is available and deal with it
					if (key.isConnectable()) {
						this.finishConnection(key);
					} else if (key.isReadable()) {
						this.read(key);
					} else if (key.isWritable()) {
						this.write(key);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void read(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		// Clear out our read buffer so it's ready for new data
		this.readBuffer.clear();
		// Attempt to read off the channel
		int numRead;
		try {
			numRead = socketChannel.read(this.readBuffer);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		if (numRead == -1) {
			return;
		}
		if(!checkPackets.isEmpty())
		{
			byte[] receivedData = new byte[readBuffer.position()];
			this.readBuffer.flip();
			this.readBuffer.get(receivedData);
			
			//Since we may get multiple responses, we parse the response
			String[] newdata = new String(receivedData).split("\n");
			int reqCount = requestCount.get(socketChannel);
			//We update the number of requests for the current socketChannel
			requestCount.put(socketChannel, reqCount+newdata.length);
			for(int j = reqCount; j < requestCount.get(socketChannel) && j >= 0; j++)
			{
				//Get a DataPacket from the local queue
				DataPacket newpacket = checkPackets.get(j);
				//Since the responses are ordered, the first response belongs to the first element
				//in the local queue
				int repCount = replicationCounter.get(newpacket);
				//Increase the replication count
				repCount = repCount + 1;
				if(newdata[j-reqCount].contains("NOT_STORED") || newdata[j-reqCount].contains("NOT_FOUND"))
				{
					newpacket.ERROR_MESSAGE = true;
					this.errormessage = (newdata[j-reqCount]+"\n").getBytes();
				}
				else
				{
					this.storedmessage = (newdata[j-reqCount]+"\n").getBytes();
				}
				//When replication counter is equal to numReplications, send the response back
				if(repCount == this.numReplications)
				{
					//remove the DataPacket's replication count from the HashMap
					replicationCounter.remove(newpacket);
					//Stop the time for Tserver as all the response/s for the request is/are received
					newpacket.Tserver = System.nanoTime() - newpacket.Tserver;
					//Stop the time for Tmw as the request is now sent back to memaslap
					newpacket.Tmw = System.nanoTime() - newpacket.Tmw;
					
					//If any response had an error message, we send the error message back
					if(newpacket.ERROR_MESSAGE){
						newpacket.Fsuccess = false;
						newpacket.manager.send(newpacket.socket, this.errormessage);
					}
					else
					{
						newpacket.manager.send(newpacket.socket, this.storedmessage);
					}
					
					//Log once every 100 iterations
					if(newpacket.manager.setcounter%100 == 0)
					{
						String logMsg = String.format("SET "+ newpacket.Tmw/1000 + " " + newpacket.Tqueue/1000 + " " + newpacket.Tserver/1000 + " " + newpacket.Fsuccess);
						newpacket.manager.myLogger.info(logMsg);
					}
					//Update the request counts for all the sockets
					for(String node: newpacket.replicaServers)
					{
						SocketChannel tmpRepSocket = AddresstoSocket.get(node);
						int tmpreqCount = requestCount.get(tmpRepSocket);
						requestCount.put(tmpRepSocket, tmpreqCount-1);
					}
					//remove the packet from the local queue as it has received all its responses
					checkPackets.remove(j);
					j = j - 1;
					reqCount = reqCount - 1;
				}
				//If replication counter hasn't reached numReplications, just update the new count
				// and continue
				else
				{
					replicationCounter.put(newpacket,repCount);
				}
			}
		}
	}


	private void write(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		if(checkPackets.size()>0){
			packet = checkPackets.get(checkPackets.size()-1);
			ByteBuffer buf = ByteBuffer.wrap(packet.data);
			socketChannel.write(buf);
			// Since the data has been written,
			// register an interest in reading on this channel
			key.interestOps(SelectionKey.OP_READ);
		}
	}

	private void finishConnection(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		// Finish the connection. If the connection operation failed
		// this will raise an IOException.
		try {
			socketChannel.finishConnect();
			key.interestOps(0);
		} catch (IOException e) {
			// Cancel the channel's registration with our selector
			System.out.println(e);
			key.cancel();
			return;
		}
	}

	//wakeup the selector
	public void modifySelector(){
		this.selector.wakeup();
	}

	private SocketChannel initiateConnection(InetAddress hostAddress, int port) throws IOException {
		// Create a non-blocking socket channel as it is asynchronous
		SocketChannel socketChannel = SocketChannel.open();
		socketChannel.configureBlocking(false);
		socketChannel.register(this.selector, SelectionKey.OP_CONNECT);
		// Kick off connection establishment
		socketChannel.connect(new InetSocketAddress(hostAddress, port));
		return socketChannel;
	}

	private Selector initSelector() throws IOException {
		// Create a new selector
		return SelectorProvider.provider().openSelector();
	}

}