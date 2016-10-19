import java.nio.channels.SocketChannel;
import java.util.List;

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
