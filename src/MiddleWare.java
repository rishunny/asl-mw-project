import java.io.IOException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;

/* This class creates the QueueManager instance and worker threads, 
 * both Synchronous and Asynchronous, for each server. 
 * It hashes each request from the Manager, based on the key, and 
 * forwards it to their respective queues.
*/
public class MiddleWare{
	private int numReplications;
	private HashMap<String, QueueManager> QueuePointer = new HashMap<String, QueueManager>();
	//This HashMaps maps the IP, Port tuple of memcached server to the Class instance that holds
	// its setqueue and getqueue

	private ConsistentHash<String> hashKeytoServer;
	//This object implements consistent hashing on the keys from the requests
	
	public MiddleWare(List<String> mcAddresses, int numThreadsPTP, int numReplications) throws UnknownHostException, IOException, NoSuchAlgorithmException{
		this.numReplications = numReplications;	
		//For all servers, we create their synchronous and asynchronous worker threads
		for(String node: mcAddresses)
		{
			QueueManager queuetoServer = new QueueManager(10000);
			QueuePointer.put(node, queuetoServer);
			AsynchronousClient newAsyncClient = new AsynchronousClient(node,this.numReplications,queuetoServer.setQueue,mcAddresses);
			queuetoServer.asyncClient = newAsyncClient;
			
			//We run the AsynchronousClient and create one instance per server
			
			new Thread(newAsyncClient).start();
			for(int i = 0; i < numThreadsPTP; i++)
			{
				new Thread(new SynchronousClient(node, queuetoServer.getQueue)).start();
				//We run the SynchronousClient and create numThreadsPTP instances per server
			}
		}


		hashKeytoServer = new ConsistentHash<String>(200,mcAddresses);
		//created an instance of the consistent hashing class with 200 virtual nodes

	}

	public void processData(DataPacket sendPacket, int count) throws IOException, InterruptedException {
		String data_string = new String(sendPacket.data).trim();
		
		//Split the request to identify SET/GET/DELETE and retrieve the key
		String command = data_string.split(" ")[0];
		byte[] key = data_string.split(" ")[1].getBytes();
		if(command.equals("set"))
		{
			//calling consistent hash and get the IP,port of the primary and the replication servers to write to
			List<String> serverAddresses = hashKeytoServer.getWithReplication(key, numReplications);
			//setting the replica servers in the DataPacket instance as well
			sendPacket.setReplicaServers(serverAddresses);
			//Start the time for Tqueue as the request is queued
			sendPacket.Tqueue = System.nanoTime();
			//put the request in the set or write queue
			QueuePointer.get(serverAddresses.get(0)).setQueue.put(sendPacket);
			//wakeup the selector
			QueuePointer.get(serverAddresses.get(0)).asyncClient.modifySelector();
			//increase the set counter from the manager instance
			sendPacket.manager.setcounter++;
		}
		else if(command.equals("get"))
		{
			//call consistent hash and get the IP,port of primary server
			String serverAddress = hashKeytoServer.get(key);
			//Start the time for Tqueue as the request is queued
			sendPacket.Tqueue = System.nanoTime();
			//put the request in the get or read queue
			QueuePointer.get(serverAddress).getQueue.put(sendPacket);
			//increase the get counter from the manager instance
			sendPacket.manager.getcounter++;
		}
		
		//deletes are handled the same way as SET
		else if(command.equals("delete"))
		{
			List<String> serverAddresses = hashKeytoServer.getWithReplication(key, numReplications);
			sendPacket.setReplicaServers(serverAddresses);
			QueuePointer.get(serverAddresses.get(0)).setQueue.put(sendPacket);
			sendPacket.Tqueue = System.nanoTime();
			QueuePointer.get(serverAddresses.get(0)).asyncClient.modifySelector();
		}
		else
		{
			System.out.println("Invalid command, please use set, get or delete");
		}
	}

}