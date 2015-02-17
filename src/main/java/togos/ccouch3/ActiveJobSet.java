package togos.ccouch3;

import java.util.HashSet;

/**
 * Can be used to:
 * - Track active jobs to prevent a job from being submitted
 *   while an identical one is already running.
 * - Allow a routine to wait until all jobs are done,
 *   e.g. before stopping the program.
 */
public class ActiveJobSet<Item>
{
	private HashSet<Item> items = new HashSet<Item>();
	
	public synchronized boolean add( Item t ) {
		if( items.contains(t) ) return false;
		items.add(t);
		return true;
	}
	
	public synchronized void replace( Item removeThis, Item addThis ) {
		items.add(addThis);
		items.remove(removeThis);
	}

	public synchronized void remove( Item t ) {
		items.remove(t);
		notifyAll();
	}
	
	public synchronized HashSet<Item> getItems() {
		return new HashSet<Item>(items);
	}
	
	public synchronized void waitUntilEmpty()
		throws InterruptedException
	{
		synchronized(this) {  while(items.size() > 0) wait(); }
	}
}
