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
	private int count = 0;
	private ArrayBlockingQueue<DataPacket> setQueue;
	private DataPacket packet;
	private ArrayList<DataPacket> checkPackets= new ArrayList<DataPacket>();
	private SocketChannel socketChannel;
	private int numReplications;
	private HashMap<String, SocketChannel> AddresstoSocket = new HashMap<String, SocketChannel>();
	// The buffer into which we'll read data when it's available
	private ByteBuffer readBuffer = ByteBuffer.allocate(2048);

	// The selector we'll be monitoring
	private Selector selector;

	// internal queue 

	public AsynchronousClient(String hostAddress, int numReplications, ArrayBlockingQueue<DataPacket> setQueue, List<String> mcAddresses) throws IOException {
		this.setQueue = setQueue;
		this.hostAddress = InetAddress.getByName(hostAddress.split(":")[0]);
		this.port = Integer.parseInt(hostAddress.split(":")[1]);
		// Create a non-blocking socket channel
		this.socketChannel = SocketChannel.open();
		this.socketChannel.configureBlocking(false);
		this.selector = this.initSelector();
		this.numReplications = numReplications;
		this.socketChannel.register(this.selector, SelectionKey.OP_CONNECT);
		if(numReplications > 0)
		{
			for(String node: mcAddresses)
			{
				InetAddress address = InetAddress.getByName(node.split(":")[0]);
				int port = Integer.parseInt(node.split(":")[1]);
				AddresstoSocket.put(node, this.initiateConnection(address, port));
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
					System.out.println("Async commands:" + new String(packet.data).trim());
					checkPackets.add(packet);
					//System.out.println("Key: "+this.socketChannel.keyFor(this.selector));
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

			key.cancel();
			System.out.println("CLOSE SOCKET 1");
			socketChannel.close();
			return;
		}

		if (numRead == -1) {
			// Remote entity shut the socket down cleanly. Do the
			// same from our end and cancel the channel.
			return;
		}
		if(!checkPackets.isEmpty())
		{
			DataPacket newpacket = checkPackets.get(0);
			String receivedData = new String(this.readBuffer.array()).trim();
			String[] newdata = receivedData.split("\n");
			System.out.println("Data at async: " + receivedData);
			if(this.numReplications == 1)
			{
				newpacket.server.send(newpacket.socket, readBuffer.array());
				if(checkPackets.size()==1)
					checkPackets.clear();
				else if(checkPackets.size()>1)
					checkPackets.remove(0);
				if(!checkPackets.isEmpty())
					newpacket = checkPackets.get(0);
			}
			else
			{
				byte[] serverresponse = "STORED\n".getBytes();
				for(int j = 0; j < newdata.length; j++)
				{
					if(!newdata[j].contains("STORED"))
					{
						System.out.println("OH NO.");
						System.out.println(newdata[j]);
						serverresponse = newdata[j].getBytes();
					}
					count++;
					System.out.println(newdata[j]);
					if(count == this.numReplications)
					{
						count = 0;
						newpacket.server.send(newpacket.socket, readBuffer.array());
						if(checkPackets.size()==1)
							checkPackets.clear();
						else if(checkPackets.size()>1)
							checkPackets.remove(0);
						if(!checkPackets.isEmpty())
							newpacket = checkPackets.get(0);
					}
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
		} catch (IOException e) {
			// Cancel the channel's registration with our selector
			System.out.println(e);
			key.cancel();
			System.out.println("CLOSE SOCKET 2");
			return;
		}

		// Register an interest in writing on this channel
		key.interestOps(SelectionKey.OP_WRITE);
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