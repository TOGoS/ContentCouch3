package togos.ccouch3.cmdstream;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import junit.framework.TestCase;

public class CmrReaderTest extends TestCase
{
	PipedOutputStream pos;
	PipedInputStream pis;
	CmdWriter w;
	CmdReader r;
	
	public void setUp() throws IOException {
		pos = new PipedOutputStream();
		pis = new PipedInputStream(pos);
		w = new CmdWriter(pos);
		r = new CmdReader(pis);
		
	}
	
	protected void assertEquals( String[] s1, String[] s2 ) {
		assertEquals( s1.length, s2.length );
		for( int i=0; i<s1.length; ++i ) assertEquals(s1[i], s2[i]);
	}
	
	protected void testCmd( String[] cmd ) throws IOException {
		w.writeCmd( cmd );
		assertEquals( cmd, r.readCmd() );
	}
	
	public void testCmd() throws IOException {
		testCmd( new String[] { "hello", "1", "2", "3" } );
	}
	
	public void testSkipNewlines() throws IOException {
		w.writeNewline();
		w.writeNewline();
		w.writeNewline();
		testCmd( new String[] { "hello", "1", "2", "3" } );
	}
	
	public void testEof() throws IOException {
		pos.close();
		assertNull( r.readCmd() );
	}
	
	public void testRead2Things() throws IOException {
		String[] thing1 = new String[] { "hello", "1", "2", "3" };
		String[] thing2 = new String[] { "blox", "of", "shox" };
		w.writeCmd( thing1 );
		w.writeCmd( thing2 );
		assertEquals( thing1, r.readCmd() );
		assertEquals( thing2, r.readCmd() );
	}
}
