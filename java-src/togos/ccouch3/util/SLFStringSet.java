package togos.ccouch3.util;

import java.io.File;

import togos.blob.ByteChunk;
import togos.blob.util.BlobUtil;
import togos.ccouch3.slf.SimpleListFile2;

public class SLFStringSet implements AddableSet<String>
{
	static final ByteChunk YES_MARKER = BlobUtil.byteChunk("Y");
	
	final File slfFile;
	
	private SimpleListFile2 slfFile2;
	
	public SLFStringSet( File slfFile ) {
		this.slfFile = slfFile;
	}
	
	protected synchronized SimpleListFile2 getSlf() {
		if( slfFile2 == null ) {
			slfFile2 = SimpleListFile2.mkSlf(slfFile);
		}
		return slfFile2;
	}
	
	@Override
	public void add(String val) {
		SimpleListFile2 c = getSlf();
		ByteChunk urnChunk = BlobUtil.byteChunk(val);
		synchronized( c ) { c.put(urnChunk, YES_MARKER); }
	}
	
	@Override
	public boolean contains(String val) {
		SimpleListFile2 c = getSlf();
		ByteChunk urnChunk = BlobUtil.byteChunk(val);
		ByteChunk storedMarker;
		synchronized( c ) { storedMarker = c.get(urnChunk); }
		return YES_MARKER.equals(storedMarker);
	}
}