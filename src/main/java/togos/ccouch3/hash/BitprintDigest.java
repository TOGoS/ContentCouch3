package togos.ccouch3.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bitpedia.util.Base32;
import org.bitpedia.util.TigerTree;

import togos.ccouch3.repo.UnsupportedSchemeException;

public class BitprintDigest extends MessageDigest
{
	public static final int HASH_SIZE = 44;
	
	public static final MessageDigestFactory FACTORY = new MessageDigestFactory() {
		@Override public MessageDigest createMessageDigest() {
			return new BitprintDigest();
		}
	};
	
	public static final HashFormatter FORMATTER = new HashFormatter() {
		@Override public String format(byte[] hash) {
			return BitprintDigest.formatUrn(hash);
		}
	};
	
	public static final StreamURNifier STREAM_URNIFIER =
			new MessageDigestStreamURNifier(FACTORY, FORMATTER);
		
	
	public static String format( byte[] hash ) {
		byte[] sha1Hash = new byte[20];
		System.arraycopy( hash, 0, sha1Hash, 0, 20);
		byte[] tigerTreeHash = new byte[24];
		System.arraycopy( hash, 20, tigerTreeHash, 0, 24);
		return Base32.encode(sha1Hash) + "." + Base32.encode(tigerTreeHash);
	}
	
	public static final Pattern URN_PATTERN = Pattern.compile("urn:bitprint:([A-Z2-7]{32})\\.([A-Z2-7]{39})");
	
	public static String formatUrn( byte[] hash ) {
		return "urn:bitprint:"+format(hash);
	}
	
	public static byte[] urnToBytes( String urn ) throws UnsupportedSchemeException {
		Matcher m = URN_PATTERN.matcher(urn);
		if( !m.matches() ) throw new UnsupportedSchemeException("Invalid Bitprint URN: "+urn);
		return joinHashes(Base32.decode(m.group(1)), Base32.decode(m.group(2)));
	}
	
	MessageDigest sha1;
	TigerTree tt;
	
	public BitprintDigest() {
		super("Bitprint");
		try {
			sha1 = MessageDigest.getInstance("SHA-1");
		} catch( NoSuchAlgorithmException e ) {
			throw new RuntimeException("Apparently SHA-1 isn't available", e);
		}
		tt   = new TigerTree();
	}
	
	protected static byte[] joinHashes( byte[] sha1Hash, byte[] tigerTreeHash ) {
		byte[] hash = new byte[44];
		System.arraycopy( sha1Hash, 0, hash, 0, 20);
		System.arraycopy( tigerTreeHash, 0, hash, 20, 24);
		return hash;
	}
	
	protected byte[] engineDigest() {
		return joinHashes( sha1.digest(), tt.digest() ); 
	}
	
	protected void engineReset() {
		sha1.reset();
		tt.reset();
	}
	
	protected void engineUpdate( byte i ) {
		sha1.update(i);
		tt.update(i);
	}
	
	protected void engineUpdate( byte[] input, int offset, int len ) {
		sha1.update(input, offset, len);
		tt.update(input, offset, len);
	}
}
