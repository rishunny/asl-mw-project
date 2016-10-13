
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;

public class SynchronousClient implements Runnable {
	// The host:port combination to connect to
	private InetAddress hostAddress;
	private int port;
	private ArrayBlockingQueue<DataPacket> getQueue;
	private DataPacket packet;
	private SocketChannel socketChannel;
	// The buffer into which we'll read data when it's available
	private ByteBuffer readBuffer = ByteBuffer.allocate(2048);


	public SynchronousClient(String hostAddress, ArrayBlockingQueue<DataPacket> getQueue) throws IOException {
		this.getQueue = getQueue;
		this.hostAddress = InetAddress.getByName(hostAddress.split(":")[0]);
		this.port = Integer.parseInt(hostAddress.split(":")[1]);
		// Create a non-blocking socket channel
		this.socketChannel = SocketChannel.open();
		this.socketChannel.configureBlocking(true);
		// Kick off connection establishment
		this.socketChannel.connect(new InetSocketAddress(this.hostAddress, this.port));
	}

	public void run() {
		while(true){	
			try {
				packet = this.getQueue.take();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				this.socketChannel.write(ByteBuffer.wrap(packet.data));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				this.socketChannel.read(readBuffer);
				packet.server.send(packet.socket, readBuffer.array());
				readBuffer.clear();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				try {
					socketChannel.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}
	



}