package togos.ccouch3.slf;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;
import togos.blob.ByteChunk;
import togos.blob.SimpleByteChunk;

@SuppressWarnings({"unchecked","rawtypes"})
public class SimpleListFile2Test extends TestCase
{
	Random r = new Random();
	
	protected ByteChunk fixedRand( int length ) {
		byte[] data = new byte[length];
		r.nextBytes(data);
		return new SimpleByteChunk(data);
	}
	
	protected ByteChunk rand( int maxLen ) {
		return fixedRand(r.nextInt(maxLen));
	}
	
	File f = new File("junk/slf-test.slf2");
	
	protected static void makeParentDirs( File f ) {
		File parentFile = f.getParentFile(); 
		if( parentFile != null && !parentFile.exists() ) {
			parentFile.mkdirs();
		}
	}
	
	public void setUp() throws IOException {
		if( f.exists() ) f.delete();
		else makeParentDirs(f);
	}
	
	public void tearDown() throws IOException {
		f.deleteOnExit();
	}
	
	public void testLongCodec() {
		byte[] buf = new byte[16];
		for( int i=0; i<100; ++i ) {
			long v = r.nextLong();
			int off = r.nextInt(8);
			SimpleListFile2.encodeLong(v, buf, off);
			assertEquals( v, SimpleListFile2.decodeLong(buf, off));
		}
	}
	
	protected void _testReadWrite( boolean lock ) throws IOException {
		if( f.exists() ) f.delete();
		
		HashMap kv = new HashMap();
		RandomAccessFileBlob blob = new RandomAccessFileBlob(f, "rw");
		SimpleListFile2 slf = new SimpleListFile2(blob, 16, lock);
		try {
			for( int i=0; i<1024; ++i ) {
				ByteChunk k = rand( 128 );
				ByteChunk v = rand( 512 );
				kv.put( k, v );
				slf.put( k, v );
				assertEquals( v, slf.get(k) );
			}
			
			for( Iterator i=kv.entrySet().iterator(); i.hasNext(); ) {
				Map.Entry e = (Map.Entry)i.next();
				assertEquals( e.getValue(), slf.get((ByteChunk)e.getKey()) );
			}
			
			blob.close();
			blob = new RandomAccessFileBlob(f, "rw");
		} finally {
			slf.close();
		}
		
		try {
			slf = new SimpleListFile2(blob, 8, lock);
			
			for( Iterator i=kv.entrySet().iterator(); i.hasNext(); ) {
				Map.Entry e = (Map.Entry)i.next();
				assertEquals( e.getValue(), slf.get((ByteChunk)e.getKey()) );
			}
		} finally {
			slf.close();
		}
	}
	
	public void testReadWrite() throws IOException {
		_testReadWrite( false );
	}
	
	public void testReadWriteWithLocks() throws IOException {
		_testReadWrite( true );
	}
	
	public void testReadMultithreadedWrite() throws IOException, InterruptedException {
		final int numThreads = 16;
		final int insertsPerThread = 256;
		final int numEntries = numThreads*insertsPerThread;
		
		final ByteChunk[] keys   = new ByteChunk[numEntries];
		final ByteChunk[] values = new ByteChunk[numEntries];
		
		for( int j=0; j<numEntries; ++j ) {
			keys[j] = fixedRand(32);
			values[j] = rand(2048);
		}
		
		if( f.exists() ) f.delete();
		
		final RandomAccessFileBlob blob = new RandomAccessFileBlob(f, "rw");
		final SimpleListFile2 slf = new SimpleListFile2(blob, 16, true);
		try {
			Thread[] threads = new Thread[numThreads];
			for( int i=0; i<numThreads; ++i ) {
				final int offset = i*insertsPerThread;
				threads[i] = new Thread() {
					public void run() {
						for( int j=0; j<insertsPerThread; ++j ) {
							slf.put( keys[j+offset], values[j+offset] );
						}
					};
				};
				threads[i].start();
			}
			
			for( int i=0; i<numThreads; ++i ) {
				threads[i].join();
			}
			
			for( int j=0; j<numEntries; ++j ) {
				assertEquals( values[j], slf.get(keys[j]) );
			}
		} finally {
			slf.close();
		}
	}
	
	public void testReadMultithreadedBulkWrite() throws IOException, InterruptedException {
		final int numThreads = 16;
		final int insertsPerThread = 256;
		final int numEntries = numThreads*insertsPerThread;
		
		final ByteChunk[] keys   = new ByteChunk[numEntries];
		final ByteChunk[] values = new ByteChunk[numEntries];
		
		for( int j=0; j<numEntries; ++j ) {
			keys[j] = fixedRand(32);
			values[j] = rand(2048);
		}
		
		if( f.exists() ) f.delete();
		
		final RandomAccessFileBlob blob = new RandomAccessFileBlob(f, "rw");
		// 7 is chosen to ensure that some indexes must be reused
		// within a chunk.
		final SimpleListFile2 slf = new SimpleListFile2(blob, 7, true);
		try {
			Thread[] threads = new Thread[numThreads];
			for( int i=0; i<numThreads; ++i ) {
				final int offset = i*insertsPerThread;
				threads[i] = new Thread() {
					public void run() {
						slf.multiPut( keys, values, offset, insertsPerThread );
					};
				};
				threads[i].start();
			}
			
			for( int i=0; i<numThreads; ++i ) {
				threads[i].join();
			}
			
			for( int j=0; j<numEntries; ++j ) {
				assertEquals( values[j], slf.get(keys[j]) );
			}
		} finally {
			slf.close();
		}
	}
}
