import java.util.concurrent.ArrayBlockingQueue;


public class QueueManager {
	public ArrayBlockingQueue<DataPacket> getQueue;
	public ArrayBlockingQueue<DataPacket> setQueue;
	
	public QueueManager(int sizeQueue)
	{
		this.getQueue = new ArrayBlockingQueue<DataPacket>(sizeQueue);
		this.setQueue = new ArrayBlockingQueue<DataPacket>(sizeQueue, true);
	}

}
