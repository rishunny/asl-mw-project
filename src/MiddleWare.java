import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;

public class MiddleWare{
	private List queue = new LinkedList();
	
	public void processData(Server server, SocketChannel socket, byte[] data, int count) throws IOException {
		byte[] dataCopy = new byte[count];
		System.arraycopy(data, 0, dataCopy, 0, count);
		
		synchronized(queue) {
			queue.add(new ServerDataEvent(server, socket, dataCopy));
			queue.notify();
		}
		AsynchronousClient a_client = new AsynchronousClient(InetAddress.getByName("192.168.0.19"), 11212, dataCopy);
		Thread t = new Thread(a_client);
		//t.setDaemon(true);
		t.start();
		a_client.send(server, socket, dataCopy);
	}
}