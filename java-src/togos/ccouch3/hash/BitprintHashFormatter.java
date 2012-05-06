package togos.ccouch3.hash;

public class BitprintHashFormatter implements HashFormatter {
	public static final BitprintHashFormatter instance = new BitprintHashFormatter();
	
	protected BitprintHashFormatter() { /* There can be only one! */ }
	
	@Override
	public String format(byte[] hash) {
		return "urn:bitprint:" + BitprintDigest.format(hash);
	}
}
