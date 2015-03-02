package togos.ccouch3;

import togos.blob.ByteChunk;
import togos.blob.util.BlobUtil;
import togos.blob.util.SimpleByteChunk;

/** These get sent through for small blobs, like encoded tree nodes. */
class SmallBlobInfo extends SimpleByteChunk implements BlobInfo
{
	public final String urn;
	
	public SmallBlobInfo( String urn, byte[] blob, int offset, int length ) {
		super(blob, offset, length);
		this.urn = urn;
		assert offset >= 0;
		assert offset <= blob.length;
		assert offset + length <= blob.length;
	}
	
	public SmallBlobInfo( String urn, ByteChunk c ) {
		this( urn, c.getBuffer(), c.getOffset(), BlobUtil.chunkLength(c) );
	}
	
	public SmallBlobInfo( String urn, byte[] blob ) {
		this( urn, blob, 0, blob.length );
	}
	
	@Override public String getUrn() { return urn; }
}
