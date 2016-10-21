import java.math.BigInteger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/*
 * This class implements consistent hashing using the MD5 hash function. 
 * It adds the servers to the circular ring and fetches the primary 
 * and replica servers from the key.
 */
public class ConsistentHash<T> {

	private final MessageDigest hashFunction;
	private final int numberOfReplicas;
	
	private final SortedMap<BigInteger, String> circle =
			new TreeMap<BigInteger, String>();
	
	//This HashMap is used to find the position of the server in a global Array List to
	//avoid searching for the server position in the List.
	private HashMap<String,Integer> Position = new HashMap<String, Integer>();
	
	//This ArrayList object is used to store the server position during the initialisation
	//as it is utilised to find the next replicated servers, located next to the primary server
	//in the circular ring, without having to hash everytime for a new key.
	private List<String> serverList = new ArrayList<String>();
	
	public ConsistentHash(int numberOfReplicas, Collection<String> nodes) throws NoSuchAlgorithmException {

		this.hashFunction = MessageDigest.getInstance("MD5");
		this.numberOfReplicas = numberOfReplicas;
		//Add all the nodes to the list of servers and add the server to the circle
		int i = 0;
		for (String node : nodes) {
			add(node);
			serverList.add(node);
			Position.put(node, i);
			i++;
		}

	}

	public void add(String node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			byte[] res = hashFunction.digest((node + "*" + i).getBytes());
			BigInteger bigres = new BigInteger(1,res);
			circle.put(bigres,node);
		}
	}
	
	//get method to obtain the primary server
	public String get(byte[] key) {
		if (circle.isEmpty()) {
			return null;
		}
		//Hash the key and look for the nearest server in the circle
		//in the clockwise direction
		byte[] res = hashFunction.digest(key);
		BigInteger bigres = new BigInteger(1,res);
		if (!circle.containsKey(bigres)) {
			SortedMap<BigInteger, String> tailMap = circle.tailMap(bigres);
			//if no server exists, choose the first server in the circular ring
			bigres = tailMap.isEmpty() ?
					circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(bigres);
	} 

	//get method to obtain the primary server as well as the secondary servers
	public List<String> getWithReplication(byte[] key, int replicationFactor) {
		List<String> result = new ArrayList<String>();
		//get the primary server first
		String primaryServer = this.get(key);
		//get the position of the primary server in the array list
		int pos = Position.get(primaryServer);
		int i = 1;
		result.add(primaryServer);
		//Now, iterate the array list in a circular fashion and get the servers next 
		//to it based on the replicationFactor
		while(i!=replicationFactor)
		{
			pos++;
			if(pos==serverList.size())
			{
				pos = 0;
			}
			result.add(serverList.get(pos));
			i++;
		}	
		return result;
	} 

}