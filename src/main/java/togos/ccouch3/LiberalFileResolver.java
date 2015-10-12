package togos.ccouch3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import togos.blob.ByteBlob;
import togos.blob.file.FileBlob;
import togos.ccouch3.repo.FileResolver;
import togos.ccouch3.repo.Repository;

/**
 * Lets you read all sorts of things.
 * Intended for use only when you trust the guy asking you for stuff.
 * This will let you resolve arbitrary files, URLs, and standard input.
 */
public class LiberalFileResolver implements FileResolver, BlobResolver
{
	protected Repository[] repos;
	protected CCouchHeadResolver headResolver;
	
	public LiberalFileResolver( Repository[] repos, CCouchHeadResolver headResolver ) {
		this.repos = repos;
		this.headResolver = headResolver;
	}
	
	@Override
	public ByteBlob getBlob(String name) throws IOException {
		if( "-".equals(name) ) {
			return new InputStreamBlob(System.in);
		}
		
		if( name.startsWith("urn:") ) {
			for( Repository r : repos ) {
				ByteBlob b = r.getBlob(name);
				if( b != null ) return b;
			}
		}
		
		if( name.startsWith("x-ccouch-head:") ) {
			return headResolver.getBlob(name);
		}
		
		// TODO: Allow opening HTTP URLs and stuff maybe?
		
		FileBlob f = new FileBlob(name);
		if( f.exists() ) return f;
		
		throw new FileNotFoundException("Couldn't find "+name);
	}
	
	@Override
	public File getFile(String name) throws FileNotFoundException {
		ByteBlob b;
		try {
			b = getBlob(name);
		} catch( IOException e ) {
			if( e instanceof FileNotFoundException ) throw (FileNotFoundException)e;
			throw new FileNotFoundException("Couldn't resolve '"+name+"' due to an IOException: "+e.getMessage());
		}
		if( b instanceof File ) return (File)b;
		throw new FileNotFoundException(name+" resolved to a "+b.getClass().getName()+", which can't be used as a file");
	}
}
