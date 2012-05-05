package togos.ccouch3.repo;

public class UnsupportedSchemeException extends StoreException
{
	private static final long serialVersionUID = 1L;

	public UnsupportedSchemeException( String message ) {
		super(message);
	}
}
