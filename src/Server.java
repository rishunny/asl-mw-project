import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.*;

public class Server implements Runnable {
	private MiddleWare myMW;
	private InetAddress hostAddress;
	private int port;

	public List<String> mcAddresses;
	static int numThreadsPTP;
	static int writeToCount;

	// The channel on which we'll accept connections
	private ServerSocketChannel serverChannel;

	// The selector we'll be monitoring
	private Selector selector;

	// The buffer into which we'll read data when it's available
	private ByteBuffer readBuffer = ByteBuffer.allocate(2048);
	// A list of PendingChange instances
	private List pendingChanges = new LinkedList();

	// Maps a SocketChannel to a list of ByteBuffer instances
	private Map pendingData = new HashMap();
	
	public Server(String Ip, int Port, List<String> mcAddresses, int numThreadsPTP, int writeToCount) throws IOException, NoSuchAlgorithmException {
		this.hostAddress = InetAddress.getByName(Ip);
		this.port = Port;
		//Stuff from the wrapper
		this.numThreadsPTP = numThreadsPTP;
		this.writeToCount = writeToCount;
		this.myMW = new MiddleWare(mcAddresses, numThreadsPTP, writeToCount);
		this.selector = this.initSelector();
	}

	public void send(SocketChannel socket, byte[] data) {
		synchronized (this.pendingChanges) {
			// And queue the data we want written
			synchronized (this.pendingData) {
				// Indicate we want the interest operation set changed
				this.pendingChanges.add(new ChangeRequest(socket, ChangeRequest.CHANGEOPS, SelectionKey.OP_WRITE));
				this.pendingData.put(socket, ByteBuffer.wrap(data));
				//System.out.println(new Timestamp(System.currentTimeMillis()) + " Data sent to server: "+new String(data));
				// Finally, wake up our selecting thread so it can make the required changes
				this.selector.wakeup();
			}
		}
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
					if (key.isAcceptable()) {
						this.accept(key);
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

	private void accept(SelectionKey key) throws IOException {
		// For an accept to be pending the channel must be a server socket channel.
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

		// Accept the connection and make it non-blocking
		SocketChannel socketChannel = serverSocketChannel.accept();
		Socket socket = socketChannel.socket();
		socketChannel.configureBlocking(false);

		// Register the new SocketChannel with our Selector, indicating
		// we'd like to be notified when there's data waiting to be read
		socketChannel.register(this.selector, SelectionKey.OP_READ);
	}

	private void read(SelectionKey key) throws IOException, InterruptedException {
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
		byte[] buff = new byte[this.readBuffer.position()];
		this.readBuffer.flip();
		this.readBuffer.get(buff);
		//System.out.println("Sent to middleware:" + new String(buff).trim());
		// Hand the data off to our worker thread
		//System.out.println(new Timestamp(System.currentTimeMillis()) + " Data read: "+ new String(buff));
		DataPacket sendPacket = new DataPacket(this, socketChannel, buff);
		this.myMW.processData(sendPacket, numRead);
		key.interestOps(0);
	}

	private void write(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		synchronized (this.pendingData) {
				ByteBuffer buf = (ByteBuffer) this.pendingData.get(socketChannel);
				socketChannel.write(buf);
				System.out.println("Sent to memaslap: "+new String(buf.array()));
				key.interestOps(SelectionKey.OP_READ);

			}

			// We wrote away all data, so we're no longer interested
			// in writing on this socket. Switch back to waiting for
			// data.
	}

	private Selector initSelector() throws IOException {
		// Create a new selector
		Selector socketSelector = SelectorProvider.provider().openSelector();

		// Create a new non-blocking server socket channel
		this.serverChannel = ServerSocketChannel.open();
		serverChannel.configureBlocking(false);

		// Bind the server socket to the specified address and port
		InetSocketAddress isa = new InetSocketAddress(this.hostAddress, this.port);
		serverChannel.socket().bind(isa);

		// Register the server socket channel, indicating an interest in 
		// accepting new connections
		serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);

		return socketSelector;
	}
	
	public static void main(String[] args) throws NoSuchAlgorithmException, IOException{
	List<String> addresses = new ArrayList<String>();
	String ip1 = "192.168.0.40:11212";
	String ip2 = "192.168.0.40:11213";
	String ip3 = "192.168.0.40:11214";
	addresses.add(ip1);
	addresses.add(ip2);
	addresses.add(ip3);
	new Thread(new Server("192.168.0.11", 9090, addresses, 4, 3)).run();
	}

}