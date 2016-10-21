
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

	@Override
	public void run() {
		while(true){	
			try {
				packet = this.getQueue.take();
				packet.Tqueue = System.nanoTime() - packet.Tqueue;
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				packet.Tserver = System.nanoTime();
				this.socketChannel.write(ByteBuffer.wrap(packet.data));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				packet.Tserver = System.nanoTime() - packet.Tserver;
				packet.Tmw = System.nanoTime() - packet.Tmw;
				readBuffer.clear();
				this.socketChannel.read(readBuffer);
				byte[] response = new byte[readBuffer.position()];
				readBuffer.flip();
				readBuffer.get(response);
				if(new String(response).equals("END\r\n"))
				{
					packet.Fsuccess = false;
				}
				packet.manager.send(packet.socket, response);
				if(packet.manager.getcounter%100 == 0)
				{
					String logMsg = String.format("GET "+ packet.Tmw/1000 + " " + packet.Tqueue/1000 + " " + packet.Tserver/1000 + " " + packet.Fsuccess);
					packet.manager.myLogger.info(logMsg);
				}
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