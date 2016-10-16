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
	private ByteBuffer storedmessage;
	private ByteBuffer errormessage;
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

	public void run() {
		while (true) {
			try {	
				packet = this.setQueue.poll();
				if(packet!=null)
				{
					checkPackets.add(packet);
					replicationCounter.put(packet, 0);
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
			// The remote forcibly closed the connection, cancel
			// the selection key and close the channel.

			//			key.cancel();
			//			System.out.println("CLOSE SOCKET 1");
			//			socketChannel.close();
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
			System.out.println("Length of response: " + newdata.length);
			//			if(this.numReplications == 1)
			//			{
			//				DataPacket newpacket = checkPackets.get(0);
			//				newpacket.server.send(newpacket.socket, readBuffer.array());
			//				if(checkPackets.size()==1)
			//					checkPackets.clear();
			//				else if(checkPackets.size()>1)
			//					checkPackets.remove(0);
			//				if(!checkPackets.isEmpty())
			//					newpacket = checkPackets.get(0);
			//
			//			}

			int reqCount = requestCount.get(socketChannel);
			requestCount.put(socketChannel, reqCount+newdata.length);
			System.out.println("Request length is: "+requestCount.get(socketChannel)+" Socket: "+socketChannel);
			//requestCount.put(socketChannel, newdata.length);
			for(int j = reqCount; j < requestCount.get(socketChannel) && j >= 0; j++)
			{
				DataPacket newpacket = checkPackets.get(j);
				int repCount = replicationCounter.get(newpacket);
				repCount = repCount + 1;
				//if(!newdata[j-reqCount].contains("STORED"))
				//{
				//newpacket.ERROR_MESSAGE = true;
				//this.errormessage = ByteBuffer.wrap(newdata[j-reqCount].getBytes());
				//System.out.println("OH NO.");
				//System.out.println(newdata[j-reqCount]);
				//}
				//else
				//{
				//this.storedmessage = ByteBuffer.wrap(newdata[j-reqCount].getBytes());
				//System.out.println(newdata[j-reqCount]);
				//}
				if(repCount == this.numReplications)
				{
					replicationCounter.remove(newpacket);
					//System.out.println("My HashMap has: " + requestCount.get(socketChannel));
					if(newpacket.ERROR_MESSAGE){
						//System.out.println("Incorrect: "+ new String(errormessage.array()));
						newpacket.server.send(newpacket.socket, this.readBuffer.array());
					}
					else
					{
						//System.out.print("Correct: "+ new String(newdata[j-reqCount].getBytes()));
						newpacket.server.send(newpacket.socket, this.readBuffer.array());
					}
					//remove loops
					for(String node: newpacket.replicaServers)
					{
						SocketChannel tmpRepSocket = AddresstoSocket.get(node);
						System.out.println("Socket: "+tmpRepSocket);
						int tmpreqCount = requestCount.get(tmpRepSocket);
						System.out.println("Request number is:" + tmpreqCount);
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
			System.out.println("CLOSE SOCKET 2");
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