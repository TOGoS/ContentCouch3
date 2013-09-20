package togos.ccouch3.repo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bitpedia.util.Base32;

import togos.blob.ByteChunk;
import togos.blob.SimpleByteChunk;
import togos.ccouch3.util.FileUtil;

public class SHA1FileRepository implements Repository
{
	protected final File dataDir;
	protected final String storeSector;
	
	Random r = new Random();
	
	public SHA1FileRepository( File dataDir, String storeSector ) {
		this.dataDir = dataDir;
		this.storeSector = storeSector;
	}
	
	Pattern SHA1EXTRACTOR = Pattern.compile("^urn:(?:sha1|bitprint):([A-Z0-9]{32})");
	
	protected File getFile( String urn ) {
		Matcher m = SHA1EXTRACTOR.matcher(urn);
		if( !m.find() ) return null;
		if( !dataDir.exists() ) return null;
		
		String sha1Base32 = m.group(1);
		
		String postSectorPath = sha1Base32.substring(0,2) + "/" + sha1Base32;
		for( File sector : dataDir.listFiles() ) {
			File blobFile = new File(sector.getPath() + "/" + postSectorPath); 
			if( blobFile.exists() ) return blobFile;
		}
		return null;
	}
	
	@Override
	public boolean contains(String urn) {
		return getFile( urn ) != null; 
	}
	
	@Override
	public ByteChunk getChunk( String urn, int maxSize ) {
		try {
			File f = getFile( urn );
			if( f == null || f.length() > maxSize ) return null;
			
			int size = (int)f.length();
			byte[] data = new byte[size];
			FileInputStream fis = new FileInputStream( f );
			int r=0;
			for( int z; r < size && (z=fis.read(data, r, size-r)) > 0; r += z);
			if( r < size ) return null;
			return new SimpleByteChunk(data);
		} catch( IOException e ) {
			return null;
		}
	}
	
	@Override
	public InputStream getInputStream( String urn ) throws FileNotFoundException {
		File f = getFile( urn );
		if( f == null ) return null;
		return new FileInputStream(f);
	}
	
	public void put(String urn, InputStream is) throws StoreException {
		try {
			if( !contains(urn) ) {
				Matcher m = SHA1EXTRACTOR.matcher(urn); 
				if( !m.find() ) {
					throw new UnsupportedSchemeException("Unsupported URN Scheme: "+urn);
				}
				String sha1Base32 = m.group(1);
				
				File tempFile = new File(dataDir + "/" + storeSector + "/." + sha1Base32 + "-" + r.nextInt(Integer.MAX_VALUE) + ".temp" );
				try {
					FileUtil.mkParentDirs( tempFile );
					FileOutputStream fos = new FileOutputStream( tempFile );
					MessageDigest digestor;
					try {
						digestor = MessageDigest.getInstance("SHA-1");
					} catch( NoSuchAlgorithmException e ) {
						throw new StoreException( "sha1-not-found-which-is-ridiculous", e );
					}
					byte[] buffer = new byte[65536];
					int z;
					while( (z = is.read(buffer)) > 0 ) {
						digestor.update( buffer, 0, z );
						fos.write( buffer, 0, z );
					}
					fos.close();
					byte[] digest = digestor.digest();
					String calculatedSha1Base32 = Base32.encode(digest);
					if( !calculatedSha1Base32.equals(sha1Base32) ) {
						throw new HashMismatchException( "Given and calculated hashes do not match" );
					}
					File finalFile = new File(dataDir + "/" + storeSector + "/" + sha1Base32.substring(0,2) + "/" + sha1Base32);
					FileUtil.mkParentDirs( finalFile );
					if( !tempFile.renameTo(finalFile) ) {
						throw new StoreException( "Failed to move temp file to final location" );
					}
				} finally {
					if( tempFile.exists() )	tempFile.delete();
				}
			}
		} catch( IOException e ) {
			throw new StoreException( "IOException while storing", e );
		} finally {
			try {
				is.close();
			} catch( IOException e ) {
			}
		}
	};
}
