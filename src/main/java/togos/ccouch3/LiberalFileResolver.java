package togos.ccouch3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import togos.blob.ByteBlob;
import togos.blob.file.FileBlob;
import togos.blob.util.SimpleByteChunk;
import togos.ccouch3.repo.FileResolver;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.util.Base64;
import togos.ccouch3.util.URIUtil;

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
			return InputStreamBlob.getStdinBlob();
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
		
		if( name.startsWith("data:") ) {
			int commaIdx = name.indexOf(",");
			if( commaIdx == -1 ) throw new FileNotFoundException("Malformed data: URI doesn't contain a comma: "+name);
			String[] meta = name.substring(5, commaIdx).split(";");
			boolean isBase64Encoded = false;
			for( String m : meta ) {
				if( "base64".equals(m) ) isBase64Encoded = true;
			}
			String encoded = name.substring(commaIdx+1);
			byte[] data = isBase64Encoded ?
				Base64.decode(encoded) :
				URIUtil.uriDecodeBytes(encoded);
			return new SimpleByteChunk(data);
		}
		
		try {
			URL url = new URL(name);
			return new URLBlob(url);
		} catch( MalformedURLException e ) {
			// Well it wasn't that, then!
		}
		
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
	
	public List<File> getFiles(String name) {
		List<File> files = new ArrayList<File>();
		for( Repository r : repos ) {
			files.addAll(r.getFiles(name));
		}
		return files;
	}
}
