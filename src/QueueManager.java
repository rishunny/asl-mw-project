import java.util.concurrent.ArrayBlockingQueue;

/*
 * This class has the read and write queues as its attributes. 
 * It also has an AsynchronousClient reference to wake up its 
 * selector.
 */

public class QueueManager {
	//read queue
	public ArrayBlockingQueue<DataPacket> getQueue;
	//write queue
	public ArrayBlockingQueue<DataPacket> setQueue;
	public AsynchronousClient asyncClient;
	
	public QueueManager(int sizeQueue)
	{
		this.getQueue = new ArrayBlockingQueue<DataPacket>(sizeQueue);
		//The second argument to the ArrayBlockingQueue specifies the access policy.
		//It is set to true to make the queues accesses by threads to be processed 
		//in FIFO order; if false the access order is unspecified
		this.setQueue = new ArrayBlockingQueue<DataPacket>(sizeQueue, true);
	}

}
