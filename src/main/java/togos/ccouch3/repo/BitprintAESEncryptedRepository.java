package togos.ccouch3.repo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.SecretKeySpec;

import togos.blob.ByteBlob;
import togos.blob.ByteChunk;
import togos.blob.util.BlobUtil;
import togos.ccouch3.hash.BitprintDigest;

class CipherSource {
	protected final SecretKeySpec keySpec;
	protected final String algo;
	public CipherSource( SecretKeySpec keySpec, String algo ) {
		this.keySpec = keySpec;
		this.algo = algo;
	}
	public Cipher getCipher(int mode) { 
		try {
			Cipher ciph = Cipher.getInstance(this.algo);
			ciph.init(mode, this.keySpec);
			return ciph;
		} catch( RuntimeException e ) {
			throw e;
		} catch( Exception e ) {
			throw new RuntimeException(e);
		}
	}
}

class CipherBlob implements ByteBlob {
	protected final ByteBlob backingBlob;
	protected final CipherSource cipherSource;
	protected final int mode;
	public CipherBlob( ByteBlob backingBlob, CipherSource cipherSource, int mode ) {
		this.backingBlob = backingBlob;
		this.cipherSource = cipherSource;
		this.mode = mode;
	}
	
	@Override public long getSize() {
		return this.backingBlob.getSize(); // Actually I'm not sure if that's right!
	}
	
	protected Cipher getCipher() {
		return cipherSource.getCipher(mode);
	}
	
	@Override public InputStream openInputStream() throws IOException {
		return new CipherInputStream(backingBlob.openInputStream(), getCipher());
	}
	
	@Override public ByteBlob slice(long offset, long length) {
		throw new UnsupportedOperationException();
	}
	
	@Override public void writeTo(OutputStream os) throws IOException {
		backingBlob.writeTo(new CipherOutputStream(os, getCipher()));
	}
}

public class BitprintAESEncryptedRepository implements Repository
{
	protected Repository backingRepo;
	protected File tempDir;
	public BitprintAESEncryptedRepository( Repository backingRepo, File tempDir, File infoDir ) {
		this.backingRepo = backingRepo;
		this.tempDir = tempDir;
	}
	
	protected File createTempFile(String pre, String suf) throws IOException {
		if( !tempDir.exists() ) {
			tempDir.mkdirs();
		}
		return File.createTempFile(pre, suf, tempDir);
	}
	
	@Override
	public File getFile(String name) throws IOException {
		throw new IOException("Can't return a file for "+name+" because it's encrypted!");
	}
	
	protected CipherSource getCipherSource(byte[] bitprint) {
		return new CipherSource(
			new SecretKeySpec(bitprint, 0, 32, "AES"),
			"AES/CBC/PKCS5Padding"
		);
	}
	
	protected CipherSource getCipherSource(String urn) {
		return getCipherSource(BitprintDigest.urnToBytes(urn));
	}
	
	@Override public boolean contains(String urn) {
		return false;
	}
	
	@Override public void put(String urn, InputStream is) throws StoreException {
		File temp = null;
		try {
			CipherSource cs = getCipherSource(urn);
			
			temp = createTempFile("ciph", ".dat");
			FileOutputStream fos = new FileOutputStream(temp);
			DigestingOutputStream encryptedDigos = new DigestingOutputStream(new BitprintDigest(), fos);
			CipherOutputStream ciphos = new CipherOutputStream(encryptedDigos, cs.getCipher(Cipher.ENCRYPT_MODE));
			DigestingOutputStream plaintextDigos = new DigestingOutputStream(new BitprintDigest(), ciphos);
			
			BlobUtil.pipe(is, plaintextDigos);
			plaintextDigos.close();
			
			byte[] plaintextBitprintBytes = plaintextDigos.digest();
			String calculatedUrn = BitprintDigest.formatUrn(plaintextBitprintBytes);
			if( !calculatedUrn.equals(urn) ) {
				throw new StoreException("Calculated URN "+calculatedUrn+" did not match expected URN "+urn);
			}
			byte[] encryptedBitprintBytes = encryptedDigos.digest();
			String encryptedUrn = BitprintDigest.formatUrn(encryptedBitprintBytes);
			System.err.println("Encrypted "+calculatedUrn+" into "+encryptedUrn+" ("+temp+")");
			
			FileInputStream fis = new FileInputStream(temp);
			try {
				backingRepo.put(encryptedUrn, fis);
			} finally {
				fis.close();
			}
		} catch( IOException e ) {
			throw new StoreException("Failed to store "+urn, e);
		} finally {
			if( temp != null ) temp.delete();
		}
	}
	
	@Override public String put(InputStream is) throws StoreException {
		try {
			File tempFile = createTempFile("plan", ".deleteme");
			try {
				FileOutputStream fos = new FileOutputStream(tempFile);
				String urn;
				try {
					DigestingOutputStream digos = new DigestingOutputStream(new BitprintDigest(), fos);
					BlobUtil.pipe(is, digos);
					urn = BitprintDigest.formatUrn(digos.digest());
				} finally {
					fos.close();
				}
				
				FileInputStream fis = new FileInputStream(tempFile);
				try {
					put( urn, fis );
				} finally {
					fis.close();
				}
				
				return urn;
			} finally {
				tempFile.delete();
			}
		} catch( IOException e ) {
			throw new StoreException("Failed to store", e);
		}
	}
	
	@Override public ByteChunk getChunk(String urn, int maxSize) {
		throw new UnsupportedOperationException();
	}

	@Override public ByteBlob getBlob(String urn) throws IOException {
		// TODO!
		throw new UnsupportedOperationException();
	}

	@Override public InputStream getInputStream(String urn) throws IOException {
		// TODO!
		throw new UnsupportedOperationException();
	}
	
	public static void main(String[] args) {
		File repoDir = new File("temp/bpaes-test-repo");
		SHA1FileRepository sha1Repo = new SHA1FileRepository(new File(repoDir, "data"), "smooth");
		BitprintAESEncryptedRepository bpAesRepo = new BitprintAESEncryptedRepository(sha1Repo, new File(repoDir, "temp"), new File(repoDir, "info"));
		try {
			String urn = bpAesRepo.put(System.in);
			System.err.println("Stored "+urn);
		} catch( StoreException e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
