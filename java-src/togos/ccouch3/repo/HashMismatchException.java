package togos.ccouch3.repo;

public class HashMismatchException extends StoreException
{
	private static final long serialVersionUID = 1L;

	public HashMismatchException( String message ) {
		super(message);
	}
}
