package togos.ccouch3.repo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import togos.blob.ByteBlob;
import togos.blob.ByteChunk;
import togos.blob.util.BlobUtil;
import togos.blob.util.SimpleByteChunk;
import togos.ccouch3.hash.BitprintDigest;
import togos.ccouch3.util.SubInputStream;

class CipherSource {
	protected static final byte[] IV = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
	protected static IvParameterSpec IV_SPEC = new IvParameterSpec(IV);
	
	protected final SecretKeySpec keySpec;
	protected final String algo;
	public CipherSource( SecretKeySpec keySpec, String algo ) {
		this.keySpec = keySpec;
		this.algo = algo;
	}
	public Cipher getCipher(int mode) { 
		try {
			Cipher ciph = Cipher.getInstance(this.algo);
			ciph.init(mode, this.keySpec, IV_SPEC);
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
	protected final long length;
	/**
	 * 
	 * @param backingBlob
	 * @param cipherSource
	 * @param mode
	 * @param length length of data being ciphered; pass -1 if unknown 
	 */
	public CipherBlob( ByteBlob backingBlob, CipherSource cipherSource, int mode, long length ) {
		this.backingBlob = backingBlob;
		this.cipherSource = cipherSource;
		this.mode = mode;
		this.length = length;
	}
	
	@Override public long getSize() {
		return this.length;
	}
	
	protected Cipher getCipher() {
		return cipherSource.getCipher(mode);
	}
	
	@Override public InputStream openInputStream() throws IOException {
		return new SubInputStream(
			new CipherInputStream(backingBlob.openInputStream(), getCipher()),
			0, length);
	}
	
	@Override public ByteBlob slice(long offset, long length) {
		throw new UnsupportedOperationException();
	}
	
	@Override public void writeTo(OutputStream os) throws IOException {
		// Because of the length thing
		throw new UnsupportedOperationException();
		//backingBlob.writeTo(new CipherOutputStream(os, getCipher()));
	}
}

class EncryptedBlobInfo {
	public final byte[] ciphertextBitprint;
	public final long plaintextLength;
	public EncryptedBlobInfo( byte[] ciphertextBitprint, long plaintextLength ) {
		this.ciphertextBitprint = ciphertextBitprint;
		this.plaintextLength = plaintextLength;
	}
}

public class BitprintAESEncryptedRepository implements Repository
{
	protected Repository backingRepo;
	protected File tempDir;
	protected File infoDir;
	protected File mapFile;
	public BitprintAESEncryptedRepository( Repository backingRepo, File tempDir, File infoDir ) {
		this.backingRepo = backingRepo;
		this.tempDir = tempDir;
		this.infoDir = infoDir;
		this.mapFile = new File(infoDir, "bpaes-v1.map");
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
	
	protected CipherSource getCipherSource(byte[] key) {
		if( key.length < 32 ) {
			throw new RuntimeException("Key isn't long enough!");
		}
		return new CipherSource(
			new SecretKeySpec(hashKeyForDataEncryption(key), 0, 32, "AES"),
			"AES/CBC/PKCS5Padding"
		);
	}
	
	protected CipherSource getCipherSource(String urn) throws UnsupportedSchemeException {
		return getCipherSource(BitprintDigest.urnToBytes(urn));
	}
	
	protected static byte[] bitprint( byte[] key ) {
		BitprintDigest dig = new BitprintDigest();
		dig.update(key);
		return dig.digest();
	}
	
	protected static byte[] modBitprint( byte[] key, byte mod ) {
		byte[] alt = new byte[key.length];
		for( int i=0; i<key.length; ++i ) alt[i] = (byte)(key[i] ^ mod);
		return bitprint(alt);
	}
	
	protected byte[] hashKeyForDataEncryption( byte[] bitprint ) {
		return modBitprint(bitprint, (byte)1);
	}
	
	protected byte[] hashKeyForMapKey( byte[] bitprint, long inputLength ) {
		byte[] mod = modBitprint(bitprint, (byte)2);
		mod[mod.length-1] ^= ((inputLength >>  0) & 0xFF);
		mod[mod.length-2] ^= ((inputLength >>  8) & 0xFF);
		mod[mod.length-3] ^= ((inputLength >> 16) & 0xFF);
		mod[mod.length-4] ^= ((inputLength >> 24) & 0xFF);
		mod[mod.length-5] ^= ((inputLength >> 32) & 0xFF);
		mod[mod.length-6] ^= ((inputLength >> 40) & 0xFF);
		mod[mod.length-7] ^= ((inputLength >> 48) & 0xFF);
		mod[mod.length-8] ^= ((inputLength >> 56) & 0xFF);
		return mod;
	}
	protected byte[] hashKeyForMapValueEncryption( byte[] bitprint ) {
		return modBitprint(bitprint, (byte)3);
	}
	protected byte[] xor( byte[] a, byte[] b ) {
		if( b.length != a.length ) throw new RuntimeException(":P");
		byte[] z = new byte[a.length];
		for( int i=0; i<a.length; ++i ) z[i] = (byte)(a[i] ^ b[i]);
		return z;
	}
	
	protected static byte[] validateBitprintSized( byte[] data, String name ) {
		if( data.length != BitprintDigest.HASH_SIZE ) {
			throw new RuntimeException(name+".length = "+data.length+", which isn't "+BitprintDigest.HASH_SIZE);
		}
		return data;
	}
	
	protected void recordStored( byte[] inputBitprint, byte[] ciphertextBitprint, long inputLength ) throws IOException {
		byte[] key = validateBitprintSized(
			hashKeyForMapKey(inputBitprint, inputLength), "map key");
		byte[] encryptedCiphertextBitprint = validateBitprintSized(
			xor(
				ciphertextBitprint,
				hashKeyForMapValueEncryption(inputBitprint)
			), "map value pad");
		
		if( !infoDir.exists() ) infoDir.mkdirs();
		FileOutputStream fos = new FileOutputStream(mapFile, true);
		try {
			FileChannel chan = fos.getChannel();
			FileLock lock = chan.lock();
			try {
				fos.write(key);
				fos.write(encryptedCiphertextBitprint);
			} finally {
				lock.release();
			}
		} finally {
			fos.close();
		}
	}
	
	protected int readFully( InputStream is, byte[] into ) throws IOException {
		int z, off = 0;
		while( off < into.length && (z = is.read(into, off, into.length-off)) > 0 ) {
			off += z;
		}
		return off;
	}
	
	protected EncryptedBlobInfo findEncryptedBitprint( byte[] inputBitprint ) throws IOException {
		byte[] key = hashKeyForMapKey(inputBitprint, 0);
		if( !mapFile.exists() ) return null;
		
		FileInputStream fis = new FileInputStream(mapFile);
		final int hashSize = BitprintDigest.HASH_SIZE;
		final int recordSize = hashSize*2;
		byte[] buf = new byte[recordSize];
		findKey: while( readFully(fis, buf) == recordSize ) {
			for( int z=0; z<hashSize-8; ++z ) {
				if( key[z] != buf[z] ) continue findKey;
			}
			long length =
				((long)((key[hashSize-1]^buf[hashSize-1]) & 0xFF) <<  0) |
				((long)((key[hashSize-2]^buf[hashSize-2]) & 0xFF) <<  8) |
				((long)((key[hashSize-3]^buf[hashSize-3]) & 0xFF) << 16) |
				((long)((key[hashSize-4]^buf[hashSize-4]) & 0xFF) << 24) |
				((long)((key[hashSize-5]^buf[hashSize-5]) & 0xFF) << 32) |
				((long)((key[hashSize-6]^buf[hashSize-6]) & 0xFF) << 40) |
				((long)((key[hashSize-7]^buf[hashSize-7]) & 0xFF) << 48) |
				((long)((key[hashSize-8]^buf[hashSize-8]) & 0xFF) << 56);
			
			byte[] mapped = new byte[hashSize];
			byte[] dec = hashKeyForMapValueEncryption(inputBitprint);
			for( int z=0; z<hashSize; ++z ) {
				mapped[z] = (byte)(buf[z+hashSize] ^ dec[z]);
			}
			return new EncryptedBlobInfo(mapped, length);
		}
		return null;
	}
	
	@Override public boolean contains(String urn) {
		byte[] bitprint;
		try {
			bitprint = BitprintDigest.urnToBytes(urn);
		} catch( UnsupportedSchemeException e1 ) {
			return false;
		}
		EncryptedBlobInfo encryptedInfo;
		try {
			encryptedInfo = findEncryptedBitprint(bitprint);
		} catch( IOException e ) {
			return false;
		}
		if( encryptedInfo == null ) return false;
		return backingRepo.contains( BitprintDigest.formatUrn(encryptedInfo.ciphertextBitprint) );
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
				throw new HashMismatchException("Calculated URN "+calculatedUrn+" did not match expected URN "+urn);
			}
			byte[] encryptedBitprintBytes = encryptedDigos.digest();
			String encryptedUrn = BitprintDigest.formatUrn(encryptedBitprintBytes);
			
			recordStored(plaintextBitprintBytes, encryptedBitprintBytes, plaintextDigos.getNumberOfBytesWritten());
			
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
		try {
			// TODO: Extract this out to somewhere.
			// Why do we even have this method?
			ByteBlob blob = getBlob(urn);
			if( blob == null ) return null;
			if( blob.getSize() > maxSize ) return null;
			
			byte[] dat = new byte[maxSize];
			InputStream is = blob.openInputStream();
			int z, read=0;
			while( (z = is.read(dat, read, dat.length-read)) > 0 ) read += z;
			if( is.read() != -1 ) return null;
			if( read == maxSize ) return new SimpleByteChunk(dat);
			byte[] copy = new byte[read];
			for( int i=0; i<read; ++i ) copy[i] = dat[i];
			return new SimpleByteChunk(copy);
		} catch( IOException e ) {
			return null;
		}
	}
	
	@Override public ByteBlob getBlob(String urn) throws IOException {
		byte[] bitprint;
		try {
			bitprint = BitprintDigest.urnToBytes(urn);
		} catch( UnsupportedSchemeException e ) {
			return null;
		}
		EncryptedBlobInfo encryptionInfo = findEncryptedBitprint(bitprint);
		if( encryptionInfo == null ) return null;
		
		ByteBlob backingBlob = backingRepo.getBlob(BitprintDigest.formatUrn(encryptionInfo.ciphertextBitprint));
		if( backingBlob == null ) return null;
		
		return new CipherBlob(
			backingBlob, getCipherSource(bitprint),
			Cipher.DECRYPT_MODE, encryptionInfo.plaintextLength);
	}
	
	@Override public InputStream getInputStream(String urn) throws IOException {
		ByteBlob b = getBlob(urn);
		if( b == null ) throw new FileNotFoundException("Blob not found: "+urn);
		return b.openInputStream();
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
