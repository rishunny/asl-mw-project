import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

/*
 * This class is responsible for communicating with the memaslap clients. 
 * It initiates the connection, gets the requests from the clients and forwards
 *  it to the Middleware object and finally sends back the response from the 
 *  worker threads to the memaslap clients. It also maintains a counter 
 *  of the number of set and get requests that is later used to sample 
 *  the requests for logging.
 */

public class Manager implements Runnable {
	private MiddleWare myMW; //Middleware instance
	private InetAddress hostAddress; 
	private int port;

	public List<String> mcAddresses;
	private int numThreadsPTP;
	private int writeToCount;

	// The channel on which connections are accepted
	private ServerSocketChannel serverChannel;

	// The selector that will be monitored
	private Selector selector;

	// The buffer into which data is read data when it's available
	private ByteBuffer readBuffer = ByteBuffer.allocate(2048);
	// A list of pending change instances
	private List pendingChanges = new LinkedList();

	// Maps a SocketChannel to a list of ByteBuffer instances
	private Map pendingData = new HashMap();
	
	//counter to keep track of the number of set requests
	public long setcounter = 0;
	
	//counter to keep track of the number of set requests
	public long getcounter = 0;
	
	public Logger myLogger = Logger.getLogger("MiddleWare");
	private SimpleFormatter simpleFormatter = new SimpleFormatter();
	
	public Manager(String Ip, int Port, List<String> mcAddresses, int numThreadsPTP, int writeToCount) throws IOException, NoSuchAlgorithmException {
		this.hostAddress = InetAddress.getByName(Ip);
		this.port = Port;
		//Stuff from the wrapper
		this.numThreadsPTP = numThreadsPTP;
		this.writeToCount = writeToCount;
		this.myMW = new MiddleWare(mcAddresses, this.numThreadsPTP, this.writeToCount);
		this.selector = this.initSelector();
		this.myLogger.setLevel(Level.INFO);
		FileHandler file = new FileHandler("setgetData.log");
		file.setFormatter(simpleFormatter);
		this.myLogger.addHandler(file);
		this.myLogger.setUseParentHandlers(false);

	}

	//Before we write, we need to know that the channel is ready for more data
	public void send(SocketChannel socket, byte[] data) {
		synchronized (this.pendingChanges) {
			// Queue the data to be written
			synchronized (this.pendingData) {
				// Set a request to change the interest operation and add the data to be written back
				this.pendingChanges.add(new ChangeRequest(socket, ChangeRequest.CHANGEOPS, SelectionKey.OP_WRITE));
				this.pendingData.put(socket, ByteBuffer.wrap(data));
				this.selector.wakeup();
			}
		}
	}

	@Override
	public void run() {
		while (true) {
			try {
				//Process any pending changes
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
		// For an accept to be pending, the channel must be a server socket channel
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

		// Accept the connection and make it non-blocking
		SocketChannel socketChannel = serverSocketChannel.accept();
		socketChannel.configureBlocking(false);

		// Register the new SocketChannel with the Selector, indicating
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
			key.channel().close();
			key.cancel();
			return;
		}
		byte[] buff = new byte[this.readBuffer.position()];
		this.readBuffer.flip();
		this.readBuffer.get(buff);
		// Prepare the data with a DataPacket instance and forward it to the 
		// instance of the Middleware
		DataPacket sendPacket = new DataPacket(this, socketChannel, buff);
		//Start the time for Tmw as the request is received
		sendPacket.Tmw = System.nanoTime();
		this.myMW.processData(sendPacket, numRead);
		// Register an interest in writing on this channel
		key.interestOps(0);
	}

	private void write(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		synchronized (this.pendingData) {
				ByteBuffer buf = (ByteBuffer) this.pendingData.get(socketChannel);
				//write back the response to the memaslap client
				socketChannel.write(buf);
				//register an interest in reading on this channel
				key.interestOps(SelectionKey.OP_READ);

			}
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
	
}