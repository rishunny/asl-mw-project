import java.nio.channels.SocketChannel;

public class DataPacket {
	public Server server;
	public SocketChannel socket;
	public byte[] data;
	
	public DataPacket(Server server, SocketChannel socket, byte[] data){
		this.server = server;
		this.socket = socket;
		this.data = data;
	}
}
