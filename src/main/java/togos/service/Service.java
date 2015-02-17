package togos.service;

public interface Service
{
	public void start();
	
	/**
	 * Tells the service to 'hurry up and quit'.
	 * The service might not stop immediately -
	 * i.e. it may run for a short while after halt() is called.
	 */
	public void halt();
	
	/**
	 * Wait for the service to be completely done.
	 * If the service is not running, it should return immediately.
	 */
	public void join() throws InterruptedException;
}
