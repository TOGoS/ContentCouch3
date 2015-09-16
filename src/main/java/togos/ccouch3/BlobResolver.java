package togos.ccouch3;

import java.io.IOException;

import togos.blob.ByteBlob;

public interface BlobResolver
{
	/**
	 * Return a blob given a name.
	 * Should never return null.
	 * May throw a IOException to indicate that the blob could not be found for some reason
	 * (FileNotFoundException is recommended for generic 'I don't have that' errors).   
	 */
	public ByteBlob getBlob(String name) throws IOException;
}
