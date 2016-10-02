
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
	private byte[] data = new byte[2048];
	// The selector we'll be monitoring
	private Selector selector;

	// The buffer into which we'll read data when it's available
	private ByteBuffer readBuffer = ByteBuffer.allocate(2048);

	// A list of PendingChange instances
	private List pendingChanges = new LinkedList();

	// Maps a SocketChannel to a list of ByteBuffer instances
	private Map pendingData = new HashMap();
	
	
	public AsynchronousClient(InetAddress hostAddress, int port, byte[] data) throws IOException {
		this.hostAddress = hostAddress;
		this.port = port;
		this.selector = this.initSelector();
		this.data = data;
	}

	public void send(Server server, SocketChannel socketserver, byte[] data) throws IOException {
		// Start a new connection
		SocketChannel socket = this.initiateConnection();
		//Add packet to the queue instead of data
		DataPacket packet = new DataPacket(server, socketserver, data);
		// And queue the data we want written
		synchronized (this.pendingData) {
			List queue = (List) this.pendingData.get(socket);
			if (queue == null) {
				queue = new ArrayList();
				this.pendingData.put(socket, queue);
			}
			queue.add(packet);
			//System.out.println("Sent data: " + new String(packet.data));
		}

		// Finally, wake up our selecting thread so it can make the required changes
		this.selector.wakeup();
	}

	public void run() {
		while (true) {
			try {
				// Process any pending changes
				synchronized (this.pendingChanges) {
					Iterator changes = this.pendingChanges.iterator();
					while (changes.hasNext()) {
						ChangeRequest change = (ChangeRequest) changes.next();
						switch (change.type) {
						case ChangeRequest.CHANGEOPS:
							SelectionKey key = change.socket.keyFor(this.selector);
							key.interestOps(change.ops);
							break;
						case ChangeRequest.REGISTER:
							change.socket.register(this.selector, change.ops);
							break;
						}
					}
					this.pendingChanges.clear();
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
		
		System.out.println("Memcached Response: " + new String(readBuffer.array()));
		synchronized (this.pendingData) {
			List queue = (List) this.pendingData.get(socketChannel);
			// Read and send until there's no more data ...
			while (!queue.isEmpty()) {
				DataPacket packet = (DataPacket) queue.get(0);
				packet.server.send(packet.socket, readBuffer.array());
				queue.remove(0);
			}
			this.pendingData.remove(socketChannel);
		}

	}


	private void write(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		synchronized (this.pendingData) {
			List queue = (List) this.pendingData.get(socketChannel);
			for (int i = 0; i < queue.size(); i++) {
				// Write until there's not more data ...
				DataPacket packet = (DataPacket) queue.get(i);
				ByteBuffer buf = ByteBuffer.wrap(packet.data);
				socketChannel.write(buf);
				//System.out.println("Command: "+new String(packet.data));
			}
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
			return;
		}
	
		// Register an interest in writing on this channel
		key.interestOps(SelectionKey.OP_WRITE);
	}

	private SocketChannel initiateConnection() throws IOException {
		// Create a non-blocking socket channel
		SocketChannel socketChannel = SocketChannel.open();
		socketChannel.configureBlocking(false);
	
		// Kick off connection establishment
		socketChannel.connect(new InetSocketAddress(this.hostAddress, this.port));
	
		// Queue a channel registration since the caller is not the 
		// selecting thread. As part of the registration we'll register
		// an interest in connection events. These are raised when a channel
		// is ready to complete connection establishment.
		synchronized(this.pendingChanges) {
			this.pendingChanges.add(new ChangeRequest(socketChannel, ChangeRequest.REGISTER, SelectionKey.OP_CONNECT));
		}
		
		return socketChannel;
	}

	private Selector initSelector() throws IOException {
		// Create a new selector
		return SelectorProvider.provider().openSelector();
	}

}