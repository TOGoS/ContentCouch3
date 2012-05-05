package togos.ccouch3.repo;

import java.io.ByteArrayInputStream;
import java.util.Random;

import junit.framework.TestCase;

import togos.ccouch3.hash.BitprintDigest;

public abstract class RepositoryTest extends TestCase
{
	abstract Repository createRepo();
	
	Repository repo;
	
	public void setUp() {
		repo = createRepo();
	}
	
	Random r = new Random();
	
	protected static String bitprintUrn( byte[] digest ) {
		return "urn:bitprint:" + BitprintDigest.format(digest);
	}
	
	protected String randomBitprintUrn() {
		byte[] bitprint = new byte[44];
		r.nextBytes( bitprint );
		return bitprintUrn(bitprint);
	}
	
	public void testRandomUrnNotPresent() {
		assertFalse(repo.contains(randomBitprintUrn()));
	}
	
	public void testStoreSomethingSmall() throws StoreException {
		byte[] b = new byte[128];
		r.nextBytes(b);
		BitprintDigest dig = new BitprintDigest();
		dig.update(b);
		String urn = bitprintUrn( dig.digest() );
		
		repo.put( urn, new ByteArrayInputStream(b) );
		assertTrue(repo.contains(urn));
	}

	public void testHashMismatch() throws StoreException {
		byte[] b = "Hello, world!".getBytes();
		
		try {
			repo.put( randomBitprintUrn(), new ByteArrayInputStream(b) );
			fail("Should've had a HashMismatchException");
		} catch( HashMismatchException e ) {
		}
	}
	
	public void testUnsupportedScheme() throws StoreException {
		byte[] b = "Hello, world!".getBytes();
		
		try {
			repo.put( "urn:carpetycarp:blah", new ByteArrayInputStream(b) );
			fail("Should've had a UnsupportedSchemeException");
		} catch( UnsupportedSchemeException e ) {
		}
	}
}
