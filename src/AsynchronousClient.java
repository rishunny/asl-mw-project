
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;

public class AsynchronousClient implements Runnable {
	// The host:port combination to connect to
	private InetAddress hostAddress;
	private int port;
//	private byte[] data = new byte[2048];
	// The selector we'll be monitoring
	private Selector selector;
	
	// internal queue 
	
	// The buffer into which we'll read data when it's available
	private ByteBuffer readBuffer = ByteBuffer.allocate(8);

	
	// create hashmap with key String(IP:PORT) value socketChannel
	
	//Do the same thing as Sync in constructor
	// pass the whole list of macAdressses
	public AsynchronousClient(InetAddress hostAddress, int port) throws IOException {
		this.hostAddress = hostAddress;
		this.port = port;
		//add setqueue and intitialise
		this.selector = this.initSelector();
		// mainConnection = this.initiateConnection() hostdress port
		// check if there is replication 
		// for loop through all macadress and call initiateConnection with IP:PORT 
		// hahsmap.put(IP:PORT, return)
//		this.data = data;
	}

//	public void send(Server server, SocketChannel socketserver, byte[] data) throws IOException {
//		// Start a new connection
//		SocketChannel socket = this.initiateConnection();
//		//Add packet to the queue instead of data
//		DataPacket packet = new DataPacket(server, socketserver, data);
//		// And queue the data we want written
//		synchronized (this.pendingData) {
//			this.pendingData.put(socket, packet);
//		}
//		// Finally, wake up our selecting thread so it can make the required changes
//		this.selector.wakeup();
//	}

	public void run() {
		while (true) {
			try {
				// Process any pending changes
//				synchronized (this.pendingChanges) {
//					Iterator changes = this.pendingChanges.iterator();
//					while (changes.hasNext()) {
//						ChangeRequest change = (ChangeRequest) changes.next();
//						switch (change.type) {
//						case ChangeRequest.CHANGEOPS:
//							SelectionKey key = change.socket.keyFor(this.selector);
//							key.interestOps(change.ops);
//							break;
//						case ChangeRequest.REGISTER:
//							change.socket.register(this.selector, change.ops);
//							break;
//						}
//					}
//					this.pendingChanges.clear();
//				}
				
				// setQueue.poll dataPocket
				// initialise GlobalDataPacket put
				// mainConnectio.keyFor(selector).register(....OP_WRITE);
				// check for replication
				// go through replicaserver list in DataPacket
				// hashmap.get(IP:PORT).keyFor(selector).register(....OP_WRITE);

				// Wait for an event one of the registered channels
				this.selector.select();// selectNow

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
			socketChannel.close();
			return;
		}

		if (numRead == -1) {
			// Remote entity shut the socket down cleanly. Do the
			// same from our end and cancel the channel.
			key.channel().close();
			key.cancel();
			return;
		}
		
		
		// check for replication
		// if its not 
		// globalDataPacket.poll();
		// globalData.server.send(global.data, ...) DataPacket type 
		// if replica
		//  increment responseCounter, 
		// if failed; responseFailed
		// reponsCounter == replica && !responceFailed => globalDataPacket.poll();
		
		
	}


	private void write(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		// packet = globalPacket.peekLast()
//			DataPacket packet = (DataPacket) this.pendingData.get(socketChannel);
			ByteBuffer buf = ByteBuffer.wrap(packet.data);
			socketChannel.write(buf);
		// We wrote away all data, so we're no longer interested
		// in writing on this socket. Switch back to waiting for
		// data.
		key.interestOps(SelectionKey.OP_READ);
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
			return;
		}
	
		// Register an interest in writing on this channel
		key.interestOps(SelectionKey.OP_WRITE);
	}

	private SocketChannel initiateConnection(arg1, arg2) throws IOException {
		// Create a non-blocking socket channel
		SocketChannel socketChannel = SocketChannel.open();
		socketChannel.configureBlocking(false);
	
		// Kick off connection establishment
		socketChannel.connect(new InetSocketAddress(arg1, arg2));
	
		// Queue a channel registration since the caller is not the 
		// selecting thread. As part of the registration we'll register
		// an interest in connection events. These are raised when a channel
		// is ready to complete connection establishment.
		
		return socketChannel;
	}

	private Selector initSelector() throws IOException {
		// Create a new selector
		return SelectorProvider.provider().openSelector();
	}

}