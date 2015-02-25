package togos.ccouch3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import togos.blob.ByteChunk;

/** These get sent through for small blobs, like encoded tree nodes. */
class SmallBlobInfo implements BlobInfo {
	public final String urn;
	public final byte[] blob;
	public final int offset, length;
	
	public SmallBlobInfo( String urn, byte[] blob, int offset, int length ) {
		assert offset >= 0;
		assert offset <= blob.length;
		assert offset + length <= blob.length;
		this.urn = urn;
		this.blob = blob;
		this.offset = offset;
		this.length = length;
	}
	
	public SmallBlobInfo( String urn, ByteChunk c ) {
		this( urn, c.getBuffer(), c.getOffset(), c.getSize() );
	}
	
	public SmallBlobInfo( String urn, byte[] blob ) {
		this( urn, blob, 0, blob.length );
	}
	
	@Override public long getSize() { return length; }
	@Override public String getUrn() { return urn; }

	@Override public InputStream openInputStream() throws IOException {
		return new ByteArrayInputStream(blob, offset, length);
	}
}
