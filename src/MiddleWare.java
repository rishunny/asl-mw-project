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
	private HashMap<String, QueueManager> QueuePointer = new HashMap<String, QueueManager>();
	private ConsistentHash<String> hashKeytoServer;
	//create HashMaps that takes key as IP, Port of memcached server that points to a new Class that holds setqueue and getqueue
	//ArrayBlockingQueues, set size as parameter. For set, arg- size, true.
	public MiddleWare(List<String> mcAddresses, int numThreadsPTP, int numReplications) throws UnknownHostException, IOException, NoSuchAlgorithmException{
		this.numReplications = numReplications;	
		for(String node: mcAddresses)
		{
			QueueManager queuetoServer = new QueueManager(10000);
			QueuePointer.put(node, queuetoServer);
			AsynchronousClient newAsyncClient = new AsynchronousClient(node,this.numReplications,queuetoServer.setQueue,mcAddresses);
			queuetoServer.asyncClient = newAsyncClient;
			new Thread(newAsyncClient).start();
			//new Thread(new AsynchronousClient(node,numReplications,queuetoServer.setQueue,mcAddresses)).start();
			for(int i = 0; i < numThreadsPTP; i++)
			{
				new Thread(new SynchronousClient(node, queuetoServer.getQueue)).start();
			}
		}


		hashKeytoServer = new ConsistentHash<String>(1,mcAddresses);
		//create an instance of the consistent hashing class

	}

	public void processData(DataPacket sendPacket, int count) throws IOException, InterruptedException {
		String data_string = new String(sendPacket.data).trim();
		//System.out.println("Command: " + data_string);
		String command = data_string.split(" ")[0];
		byte[] key = data_string.split(" ")[1].getBytes();
		if(command.equals("set"))
		{
			//call consistent hash and get the IP,port
			//System.out.println("Command set: "+ data_string);
			List<String> serverAddresses = hashKeytoServer.getWithReplication(key, numReplications);
			sendPacket.setReplicaServers(serverAddresses);
			System.out.println(serverAddresses);
			QueuePointer.get(serverAddresses.get(0)).setQueue.put(sendPacket);
			QueuePointer.get(serverAddresses.get(0)).asyncClient.modifySelector();
		}
		else if(command.equals("get"))
		{
			//call consistent hash and get the list of IP,port for replications
			//System.out.println("Command get: "+ data_string);
			String serverAddress = hashKeytoServer.get(key);
			QueuePointer.get(serverAddress).getQueue.put(sendPacket);
		}
		else if(command.equals("delete"))
		{
		}
		else
		{
			System.out.println("Invalid command, please use set, get or delete");
		}
	}

}