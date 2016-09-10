package togos.ccouch3.repo;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import togos.blob.ByteBlob;

public class BitprintAESEncryptedRepositoryTest extends RepositoryTest
{ 
	SHA1FileRepository backingRepo;
	LoggingRepository backingLoggingRepo;
	BitprintAESEncryptedRepository bpAesRepo;
	
	@Override public Repository createRepo() {
		File repoDir = new File("temp/bpaes-test-repo");
		backingRepo = new SHA1FileRepository(repoDir, "smooth");
		backingLoggingRepo = new LoggingRepository(backingRepo);
		bpAesRepo = new BitprintAESEncryptedRepository(
			backingLoggingRepo, new File(repoDir, "temp"), new File(repoDir, "info"));
		return bpAesRepo;
	}
	
	public void testAThing() throws StoreException, IOException {
		byte[] data = new byte[33];
		Random r = new Random();
		r.nextBytes(data);
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		String urn = bpAesRepo.put(bais);
		
		assertEquals(1, backingLoggingRepo.events.size());
		LoggingRepository.Event evt = backingLoggingRepo.events.get(0);
		String backingBlobUrn = evt.urn;
		
		ByteBlob backingBlob = backingRepo.getBlob(backingBlobUrn);
		assertTrue(backingBlob.getSize() >= 32);
		
		ByteBlob retrievedBlob = bpAesRepo.getBlob(urn);
		assertNotNull("Failed to load blob "+urn, retrievedBlob);
		assertEquals( data.length, retrievedBlob.getSize() );
		InputStream retrievedIs = retrievedBlob.openInputStream();
		int b, i=0;
		while( (b = retrievedIs.read()) > 0 ) {
			if( data[i] != (byte)b ) fail("Byte at offset "+i+" differs between stored/retrieved data; expected "+data[i]+", got "+b);
			++i;
		}
	}
}
