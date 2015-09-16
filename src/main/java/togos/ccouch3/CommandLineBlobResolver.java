package togos.ccouch3;

import java.io.FileNotFoundException;
import java.io.IOException;

import togos.blob.ByteBlob;
import togos.blob.file.FileBlob;

public class CommandLineBlobResolver implements BlobResolver
{
	@Override
	public ByteBlob getBlob(String name) throws IOException {
		if( "-".equals(name) ) {
			return new InputStreamBlob(System.in);
		}
		
		FileBlob fb = new FileBlob(name);
		if( fb.exists() ) return fb;
		
		// TODO: Support finding x-ccouch-heads, HTTP URLs, etc.
		
		throw new FileNotFoundException(name);
	}
}
