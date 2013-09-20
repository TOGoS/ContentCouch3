package togos.ccouch3.download;

import java.util.concurrent.LinkedBlockingQueue;

public class BlockingNonBlockingQueue<T> implements BlockingQueue<T>
{
	final int maxSizeBeforePutBlocks;
	final LinkedBlockingQueue<T> q = new LinkedBlockingQueue<T>();
	
	public BlockingNonBlockingQueue( int maxSize ) {
		this.maxSizeBeforePutBlocks = maxSize;
	}
	
	@Override public void put(T t) {
		synchronized(this) {
			while( size() > maxSizeBeforePutBlocks ) wait();
				
			}
		}
	}
}
