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

public class AsynchronousClient implements Runnable {
	//Global ArrayList of type datapacket, store everything from queue here
	//Delete data after each request has been satisfied
	//For replication, have a HashMap
	private InetAddress hostAddress;
	private int port;
	private ArrayBlockingQueue<DataPacket> setQueue;
	private DataPacket packet;
	private ArrayList<DataPacket> checkPackets= new ArrayList<DataPacket>();
	private SocketChannel socketChannel;
	private int numReplications;
	private HashMap<SocketChannel, Integer> requestCount = new HashMap<SocketChannel, Integer>();
	private HashMap<DataPacket, Integer> replicationCounter = new HashMap<DataPacket, Integer>();
	private HashMap<String, SocketChannel> AddresstoSocket = new HashMap<String, SocketChannel>();
	private ByteBuffer readBuffer = ByteBuffer.allocate(2048);
	private Selector selector;
	private byte[] storedmessage;
	private byte[] errormessage;
	// internal queue 
	//make HashMap of counter and socket 
	//HashMap of datapacket and global replication
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
				packet = this.setQueue.poll();
				if(packet!=null)
				{
					checkPackets.add(packet);
					packet.Tqueue = System.nanoTime() - packet.Tqueue;
					replicationCounter.put(packet, 0);
					packet.Tserver = System.nanoTime();
					this.socketChannel.write(ByteBuffer.wrap(packet.data));
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
			// Remote entity shut the socket down cleanly. Do the
			// same from our end and cancel the channel.
			return;
		}
		if(!checkPackets.isEmpty())
		{
			byte[] receivedData = new byte[readBuffer.position()];
			this.readBuffer.flip();
			this.readBuffer.get(receivedData);
			//System.out.println("Data from memcached: "+new String(receivedData));
			String[] newdata = new String(receivedData).split("\n");
			//			if(newdata.length > 1)
			//				System.out.println("Length of response: " + newdata.length);
			int reqCount = requestCount.get(socketChannel);
			requestCount.put(socketChannel, reqCount+newdata.length);
			for(int j = reqCount; j < requestCount.get(socketChannel) && j >= 0; j++)
			{
				DataPacket newpacket = checkPackets.get(j);
				int repCount = replicationCounter.get(newpacket);
				repCount = repCount + 1;
				if(newdata[j-reqCount].contains("NOT_STORED") || newdata[j-reqCount].contains("NOT_FOUND"))
				{
					newpacket.ERROR_MESSAGE = true;
					this.errormessage = (newdata[j-reqCount]+"\n").getBytes();
					//System.out.println(newdata[j-reqCount]);
				}
				else
				{
					this.storedmessage = (newdata[j-reqCount]+"\n").getBytes();
					//System.out.println(newdata[j-reqCount]);
				}
				if(repCount == this.numReplications)
				{
					replicationCounter.remove(newpacket);
					newpacket.Tserver = System.nanoTime() - newpacket.Tserver;
					newpacket.Tmw = System.nanoTime() - newpacket.Tmw;
					if(newpacket.ERROR_MESSAGE){
						//System.out.println("Incorrect: "+ new String(errormessage.array()));
						newpacket.Fsuccess = false;
						newpacket.manager.send(newpacket.socket, this.errormessage);
					}
					else
					{
						//System.out.print("Correct: "+ new String(newdata[j-reqCount].getBytes()));
						newpacket.manager.send(newpacket.socket, this.storedmessage);
					}
					if(newpacket.manager.setcounter%100 == 0)
					{
						// String logMsg = String.format("SET %d %d %d %d", , time2);
						String logMsg = String.format("SET "+ newpacket.Tmw/1000 + " " + newpacket.Tqueue/1000 + " " + newpacket.Tserver/1000 + " " + newpacket.Fsuccess);
						newpacket.manager.myLogger.info(logMsg);
					}

					//remove loops
					for(String node: newpacket.replicaServers)
					{
						SocketChannel tmpRepSocket = AddresstoSocket.get(node);
						int tmpreqCount = requestCount.get(tmpRepSocket);
						requestCount.put(tmpRepSocket, tmpreqCount-1);
					}
					checkPackets.remove(j);
					j = j - 1;
					reqCount = reqCount - 1;
				}
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
			//			System.out.println("Data written:" + new String(packet.data) + "Key: " + socketChannel);
			socketChannel.write(buf);
			// We wrote away all data, so we're no longer interested
			// in writing on this socket. Switch back to waiting for
			// data.
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

	public void modifySelector(){
		this.selector.wakeup();
	}

	private SocketChannel initiateConnection(InetAddress hostAddress, int port) throws IOException {
		// Create a non-blocking socket channel
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