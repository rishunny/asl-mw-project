
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;

/*
 * This class gets the requests from the read queue and processes them synchronously, 
 * and fetches the response from the corresponding server it is connected to. 
 * The response is then sent back through the send method from the Manager
 *  instance in the request packet.
 */

public class SynchronousClient implements Runnable {
	// The host:port combination to connect to
	private InetAddress hostAddress;
	private int port;
	private ArrayBlockingQueue<DataPacket> getQueue;
	private DataPacket packet;
	private SocketChannel socketChannel;
	// The buffer into which the data is read when it's available
	private ByteBuffer readBuffer = ByteBuffer.allocate(2048);

	public SynchronousClient(String hostAddress, ArrayBlockingQueue<DataPacket> getQueue) throws IOException {
		this.getQueue = getQueue;
		this.hostAddress = InetAddress.getByName(hostAddress.split(":")[0]);
		this.port = Integer.parseInt(hostAddress.split(":")[1]);
		// Create a blocking socket channel as it is synchronous
		this.socketChannel = SocketChannel.open();
		this.socketChannel.configureBlocking(true);
		// Kick off connection establishment
		this.socketChannel.connect(new InetSocketAddress(this.hostAddress, this.port));
	}

	@Override
	public void run() {
		while(true){	
			try {
				//get the DataPacket instance from the queue
				packet = this.getQueue.take();
				//Stop the time for Tqueue as the request is dequeued
				packet.Tqueue = System.nanoTime() - packet.Tqueue;
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				//Start the time for Tserver as the request is forwarded to the server
				packet.Tserver = System.nanoTime();
				this.socketChannel.write(ByteBuffer.wrap(packet.data));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				//Stop the time for Tserver as the response for the request is received
				packet.Tserver = System.nanoTime() - packet.Tserver;
				//Stop the time for Tmw as the request is now sent back to memaslap
				packet.Tmw = System.nanoTime() - packet.Tmw;
				
				// Clear out our read buffer so it's ready for new data
				readBuffer.clear();
				this.socketChannel.read(readBuffer);
				byte[] response = new byte[readBuffer.position()];
				readBuffer.flip();
				readBuffer.get(response);
				//Parse the response to see if the request succeeded
				if(new String(response).equals("END\r\n"))
				{
					packet.Fsuccess = false;
				}
				//Send the response back through the Manager instance in the DataPacket
				packet.manager.send(packet.socket, response);
				
				//Log once every 100 iterations
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