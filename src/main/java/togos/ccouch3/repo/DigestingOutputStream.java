package togos.ccouch3.repo;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;

public class DigestingOutputStream extends OutputStream
{
	protected final MessageDigest digest;
	protected long length = 0;
	protected final OutputStream backingOutputStream;
	
	public DigestingOutputStream( MessageDigest digest, OutputStream backingOutputStream ) {
		this.digest = digest;
		this.backingOutputStream = backingOutputStream;
	}
	
	@Override public void write(int b) throws IOException {
		digest.update((byte)b);
		backingOutputStream.write(b);
		++length;
	}
	
	@Override public void write(byte[] b, int off, int len) throws IOException {
		digest.update(b, off, len);
		backingOutputStream.write(b, off, len);
		length += len;
	}
	
	@Override public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}
	
	public MessageDigest getMessageDigest() {
		return digest;
	}
	public byte[] digest() {
		return digest.digest();
	}
	public long getNumberOfBytesWritten() {
		return length;
	}
	
	@Override public void close() throws IOException {
		backingOutputStream.close();
	}
}
