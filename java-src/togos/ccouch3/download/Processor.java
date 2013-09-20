package togos.ccouch3.download;

abstract class Processor implements Runnable
{
	protected abstract void step() throws InterruptedException;
	
	public void run() {
		try {
			while(true) step();
		} catch( InterruptedException e ) {
			Thread.currentThread().interrupt();
			System.err.println(Thread.currentThread().getName()+" unexpectedly interrupted");
		}
	}
}
