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
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bitpedia.util.Base32;

import togos.blob.ByteChunk;
import togos.blob.file.FileBlob;
import togos.blob.util.SimpleByteChunk;
import togos.ccouch3.util.FileUtil;

public class SHA1FileRepository implements Repository
{
	protected final File dataDir;
	protected final String storeSector;
	
	Random r = new Random();
	
	/**
	 * @param dataDir repository directory
	 * @param storeSector name of sector within which new data should be saved; if null, this object
	 *   acts read-only and will not allow data to be stored.
	 */
	public SHA1FileRepository( File dataDir, String storeSector ) {
		this.dataDir = dataDir;
		this.storeSector = storeSector;
	}
	
	Pattern SHA1EXTRACTOR = Pattern.compile("^urn:(?:sha1|bitprint):([A-Z0-9]{32})");
	
	@Override public FileBlob getBlob( String urn ) {
		Matcher m = SHA1EXTRACTOR.matcher(urn);
		if( !m.find() ) return null;
		if( !dataDir.exists() ) return null;
		
		String sha1Base32 = m.group(1);
		
		String postSectorPath = sha1Base32.substring(0,2) + "/" + sha1Base32;
		
		File[] sectorFileList = dataDir.listFiles();
		if( sectorFileList == null ) return null;
		
		for( File sector : sectorFileList ) {
			FileBlob blobFile = new FileBlob(sector, postSectorPath); 
			if( blobFile.exists() ) return blobFile;
		}
		return null;
	}
	
	protected FileBlob _getFile( String urn ) { return getBlob(urn); }
	
	@Override public FileBlob getFile( String urn ) throws FileNotFoundException {
		FileBlob f = _getFile(urn);
		if( f == null ) throw new FileNotFoundException(urn+" not found in repository");
		return f;
	}
	
	@Override public boolean contains(String urn) {
		return _getFile( urn ) != null; 
	}
	
	@Override public ByteChunk getChunk( String urn, int maxSize ) {
		try {
			File f = _getFile( urn );
			if( f == null || f.length() > maxSize ) return null;
			
			int size = (int)f.length();
			byte[] data = new byte[size];
			FileInputStream fis = new FileInputStream( f );
			try {
				int r=0;
				for( int z; r < size && (z=fis.read(data, r, size-r)) > 0; r += z);
				if( r < size ) return null;
				return new SimpleByteChunk(data);
			} finally {
				fis.close();
			}
		} catch( IOException e ) {
			return null;
		}
	}
	
	@Override public InputStream getInputStream( String urn ) throws IOException {
		FileBlob f = _getFile( urn );
		if( f == null ) throw new FileNotFoundException();
		return f.openInputStream();
	}
	
	public String _put(String urn, InputStream is) throws StoreException {
		if( storeSector == null ) {
			throw new StoreException("Repository is read-only");
		}
		
		try {
			if( urn != null && contains(urn) ) {
				return urn;
			} else {
				String tempFileName1;
				String expectedSha1Base32;
				if( urn == null ) {
					tempFileName1 = UUID.randomUUID().toString();
					expectedSha1Base32 = null;
				} else {
					Matcher m = SHA1EXTRACTOR.matcher(urn); 
					if( !m.find() ) {
						throw new UnsupportedSchemeException("Unsupported URN Scheme: "+urn);
					}
					expectedSha1Base32 = m.group(1);
					tempFileName1 = expectedSha1Base32;
				}
				
				File tempFile = new File(dataDir + "/" + storeSector + "/." + tempFileName1 + "-" + r.nextInt(Integer.MAX_VALUE) + ".temp" );
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
					if( expectedSha1Base32 != null && !calculatedSha1Base32.equals(expectedSha1Base32) ) {
						throw new HashMismatchException( "Given and calculated hashes do not match" );
					}
					File finalFile = new File(dataDir + "/" + storeSector + "/" + calculatedSha1Base32.substring(0,2) + "/" + calculatedSha1Base32);
					FileUtil.mkParentDirs( finalFile );
					if( finalFile.exists() ) {
						tempFile.delete();
						return "urn:sha1:"+calculatedSha1Base32;
					}
					if( !tempFile.renameTo(finalFile) ) {
						throw new StoreException( "Failed to move temp file to final location" );
					}
					return "urn:sha1:"+calculatedSha1Base32;
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
	
	@Override public void put(String urn, InputStream is) throws StoreException {
		_put(urn, is);
	}
	@Override public String put(InputStream is) throws StoreException {
		return _put(null, is);
	}
	
	public String toString() {
		return getClass().getName()+"( dataDir @ '"+dataDir+"' )";
	}
}
