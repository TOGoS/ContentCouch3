package togos.ccouch3;

import java.util.HashSet;

public class TaskCounter<Task>
{
	private HashSet<Task> items = new HashSet<Task>();
	
	public synchronized void add( Task t ) {
		items.add(t);
	}
	
	public synchronized void replace( Task removeThis, Task addThis ) {
		items.add(addThis);
		items.remove(removeThis);
	}

	public synchronized void remove( Task t ) {
		items.remove(t);
		notifyAll();
	}
	
	public synchronized HashSet<Task> getItems() {
		return new HashSet<Task>(items);
	}
	
	public synchronized void waitUntilEmpty()
		throws InterruptedException
	{
		synchronized(this) {  while(items.size() > 0) wait(); }
	}
}
