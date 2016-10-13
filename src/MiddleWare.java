import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MiddleWare{
	private int numReplications;
	private HashMap<String, QueueManager> QueuePointer;
	private ConsistentHash<String> hashKeytoServer;
	//create HashMaps that takes key as IP, Port of memcached server that points to a new Class that holds setqueue and getqueue
	//ArrayBlockingQueues, set size as parameter. For set, arg- size, true.
	public MiddleWare(List<String> mcAddresses, int numThreadsPTP, int numReplications) throws UnknownHostException, IOException, NoSuchAlgorithmException{
		this.numReplications = numReplications;
		for(String node: mcAddresses)
		{
			QueueManager queuetoServer = new QueueManager(1000);
			QueuePointer.put(node, queuetoServer);
			for(int i = 0; i < numThreadsPTP; i++)
			{
				new Thread(new SynchronousClient(node,queuetoServer.getQueue)).start();
			}
		}


		hashKeytoServer = new ConsistentHash<String>(200,mcAddresses);
		//create an instance of the consistent hashing class

	}

	public void processData(DataPacket sendPacket, int count) throws IOException, InterruptedException {
		byte[] dataCopy = new byte[count];
		System.arraycopy(sendPacket.data, 0, dataCopy, 0, count);
		String data_string = new String(dataCopy, "ASCII");
		String command = data_string.split(" ")[0];
		byte[] key = data_string.split(" ")[1].getBytes();
		System.out.println("Command: " + command);
		switch(command){
		case "set":
			//call consistent hash and get the IP,port
			String serverAddress = hashKeytoServer.get(key);
			QueuePointer.get(key).setQueue.put(sendPacket);
		case "get":
			List<String> serverAddresses = hashKeytoServer.getWithReplication(key, numReplications);
			QueuePointer.get(key).getQueue.put(sendPacket);
			//call consistent hash and get the IP,port
			//manage.get(hashvalue).getqueue.put(datapacket);
		case "delete":
			break;
		default:
			System.out.println("Invalid command, please use set, get or delete");
			break;
		}
	}

}