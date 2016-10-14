import java.nio.channels.SocketChannel;
import java.util.List;

public class DataPacket {
	public Server server;
	public SocketChannel socket;
	public byte[] data;
	public List<String> replicaServers;
	public DataPacket(Server server, SocketChannel socket, byte[] data){
		this.server = server;
		this.socket = socket;
		this.data = data;
	}
	
	public void setReplicaServers(List<String> servers)
	{
		this.replicaServers = servers;
	} 
}
