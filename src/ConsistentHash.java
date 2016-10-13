import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
public class ConsistentHash<T> {

	private final MessageDigest hashFunction;
	private final int numberOfReplicas;
	private final SortedMap<BigInteger, String> circle =
			new TreeMap<BigInteger, String>();

	public ConsistentHash(int numberOfReplicas, Collection<String> nodes) throws NoSuchAlgorithmException {

		this.hashFunction = MessageDigest.getInstance("MD5");
		this.numberOfReplicas = numberOfReplicas;

		for (String node : nodes) {
			add(node);
		}
	}

	public void add(String node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			byte[] res = hashFunction.digest((node + "*" + i).getBytes());
			BigInteger bigres = new BigInteger(1,res);
			circle.put(bigres,node);
		}
	}


	public String get(byte[] key) {
		if (circle.isEmpty()) {
			return null;
		}
		byte[] res = hashFunction.digest(key);
		BigInteger bigres = new BigInteger(1,res);
		if (!circle.containsKey(bigres)) {
			SortedMap<BigInteger, String> tailMap = circle.tailMap(bigres);
			bigres = tailMap.isEmpty() ?
					circle.firstKey() : tailMap.firstKey();
		}
		System.out.println(bigres);
		return circle.get(bigres);
	} 

	public List<String> getWithReplication(byte[] key, int replicationFactor) {
		List<String> result = new ArrayList<String>();
		int i = 1;
		HashMap<String, Boolean> addedHash = new HashMap<String, Boolean>();
		if (circle.isEmpty()) {
			return null;
		}
		SortedMap<BigInteger, String> tailMap = new TreeMap<BigInteger, String>();
		byte[] res = hashFunction.digest(key);
		BigInteger bigres = new BigInteger(1,res);
		if (!circle.containsKey(bigres)) {
			tailMap = circle.tailMap(bigres);
			bigres = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();  
		}
		else{
			tailMap = circle.tailMap(bigres);
		}
		addedHash.put(circle.get(bigres), true);
		result.add(circle.get(bigres));
		for(Map.Entry<BigInteger,String> entry : tailMap.entrySet()) {
			//BigInteger nextval = entry.getKey();
			String value = entry.getValue();
			if(addedHash.get(value) == null)
			{
				addedHash.put(value, true);
				result.add(value);
				i++;
			}
			if(i == replicationFactor)
				break;
		}
		if(i < replicationFactor)
		{
			tailMap = circle.tailMap(circle.firstKey());
			for(Map.Entry<BigInteger,String> entry : tailMap.entrySet()) {
				//BigInteger nextval = entry.getKey();
				String value = entry.getValue();
				if(addedHash.get(value) == null)
				{
					addedHash.put(value, true);
					result.add(value);
					i++;
				}
				if(i == replicationFactor)
					break;
			}
		}
		return result;
	} 

//	public static void main(String[] args) throws NoSuchAlgorithmException{
//		List<String> addresses = new ArrayList<String>();
//		String ip1 = "192.168.1.1:8080";
//		String ip2 = "192.168.1.2:8080";
//		String ip3 = "192.168.1.3:8080";
//		String ip4 = "192.168.1.4:8080";
//		String ip5 = "192.168.1.5:8080";
//		String ip6 = "192.168.1.6:8080";
//		addresses.add(ip1);
//		addresses.add(ip2);
//		addresses.add(ip3);
//		addresses.add(ip4);
//		addresses.add(ip5);
//		addresses.add(ip6);
//		//System.out.println(addresses);
//		ConsistentHash<String> hash = new ConsistentHash<String>(200,addresses);
//		for(int i = 0; i < 10000; i++)
//		{
//			String uuid = UUID.randomUUID().toString();
//			byte[] random = uuid.getBytes();
//			System.out.println(hash.getWithReplication(random,3));
//			//System.out.println(hash.get(random));
//		}
//	}

}