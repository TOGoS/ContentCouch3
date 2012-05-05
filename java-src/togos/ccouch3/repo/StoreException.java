package togos.ccouch3.repo;

public class StoreException extends Exception
{
	private static final long serialVersionUID = 1L;
	
	public StoreException( String message ) {
		super(message);
	}

	public StoreException( String message, Throwable cause ) {
		super(message, cause);
	}
}
