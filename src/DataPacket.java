import java.nio.channels.SocketChannel;
import java.util.List;

/*
 * This class represents the request in the architecture diagram.
 *  It has an instance of the Manager class and the socket channel 
 *  connecting to the memaslap client. It also contains the actual r
 *  ead/write request, the servers it has to write to, in case of 
 *  replication, and also stores the instrumentation
 *   measurements (e.g. Tmw, Tqueue, Tserver and Fsuccess) 
 * */

public class DataPacket {
	public Manager manager;
	public SocketChannel socket;
	public byte[] data;
	public List<String> replicaServers;
	public boolean ERROR_MESSAGE;
	public long Tmw;
	public long Tserver;
	public long Tqueue;
	public boolean Fsuccess;
	
	public DataPacket(Manager manager, SocketChannel socket, byte[] data){
		this.manager = manager;
		this.socket = socket;
		this.data = data;
		this.ERROR_MESSAGE = false;
		this.Fsuccess = true;
	}
	
	public void setReplicaServers(List<String> servers)
	{
		this.replicaServers = servers;
	} 
}
