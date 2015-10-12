package togos.ccouch3;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;
import togos.blob.ByteChunk;
import togos.blob.util.BlobUtil;
import togos.ccouch3.util.FileUtil;

public class HeadManagerTest extends TestCase
{
	File headRoot = new File("temp/test-repo/heads");
	HeadManager hman;
	
	public void setUp() {
		FileUtil.deltree(headRoot);
		hman = new HeadManager(headRoot);
	}
	
	ByteChunk HEAD1 = BlobUtil.byteChunk("Won");
	ByteChunk HEAD2 = BlobUtil.byteChunk("Too");
	ByteChunk HEAD3 = BlobUtil.byteChunk("Tree");
	
	public void testNoHeads() throws IOException {
		assertNull( hman.getLatestFile("zap/hap/tap") );
		assertNull( hman.getLatest("trap") );
	}
	
	public void testNewHead() throws IOException {
		assertEquals( 1, hman.addHead("zap/hap/tap", 0, HEAD1) );
		assertNotNull( hman.getLatestFile("zap/hap/tap") );
		assertEquals( "1", hman.getLatestFile("zap/hap/tap").getName() );
		assertEquals( HEAD1, hman.getLatest("zap/hap/tap") );
		// Parent and sub-directories should still be treated as empty:
		assertNull( hman.getLatest("zap/hap/tap/fap") );
		assertNull( hman.getLatest("zap/hap") );
		assertNull( hman.getLatest("zap") );
		
		// Adding the same head again should not create a new file:
		assertEquals( 1, hman.addHead("zap/hap/tap", 0, HEAD1) );
		assertEquals( "1", hman.getLatestFile("zap/hap/tap").getName() );
		assertEquals( HEAD1, hman.getLatest("zap/hap/tap") );
		assertNull( hman.getLatest("zap/hap") );
		
		// But adding a different one should:
		assertEquals( 2, hman.addHead("zap/hap/tap", 0, HEAD2) );
		assertEquals( "2", hman.getLatestFile("zap/hap/tap").getName() );
		assertEquals( HEAD2, hman.getLatest("zap/hap/tap") );
		assertNull( hman.getLatest("zap/hap") );

		// Adding it again should keep the same ID:
		assertEquals( 2, hman.addHead("zap/hap/tap", 0, HEAD2) );
		assertEquals( "2", hman.getLatestFile("zap/hap/tap").getName() );
		assertEquals( HEAD2, hman.getLatest("zap/hap/tap") );
		assertNull( hman.getLatest("zap/hap") );
		
		// Unless we force a higher one:
		assertEquals( 3, hman.addHead("zap/hap/tap", 3, HEAD2) );
		assertEquals( "3", hman.getLatestFile("zap/hap/tap").getName() );
		assertEquals( HEAD2, hman.getLatest("zap/hap/tap") );
		assertNull( hman.getLatest("zap/hap") );
	}
}
