package togos.ccouch3.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import junit.framework.TestCase;

public class SubInputStreamTest extends TestCase
{
	public void testSliceSomething() throws IOException {
		byte[] buffer = new byte[4096];
		for( int i=0; i<4096; ++i ) buffer[i] = (byte)i;
		ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
		SubInputStream sis = new SubInputStream(bais, 12, 4000);
		
		int readFromSis = 0;
		readFromSis += sis.skip(13);
		int red = sis.read();
		assertEquals(12+13, red);
		++readFromSis;
		byte[] readBuf = new byte[128];
		int z = sis.read(readBuf, 1, readBuf.length-1);
		assertTrue( z > 0 );
		readFromSis += z;
		for( int i=0; i<z; ++i ) {
			assertEquals(12+13+1+i, readBuf[1+i] & 0xFF);
		}
		while( (z = sis.read(readBuf)) > 0 ) {
			readFromSis += z;
		}
		assertEquals(readFromSis, 4000);
		sis.close();
	}
}
