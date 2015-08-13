package togos.ccouch3;

import java.io.IOException;

public interface Logger
{
	/**
	 * When logging a large object, use this to avoid cluttering up the main log.
	 * It will return a string identifying the place where the data was stored
	 * so the user can check it.
	 */
	public String dumpToLog( byte[] data ) throws IOException;
}
