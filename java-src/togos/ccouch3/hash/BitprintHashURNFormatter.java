package togos.ccouch3.hash;

public class BitprintHashURNFormatter implements HashFormatter {
	public static final BitprintHashURNFormatter INSTANCE = new BitprintHashURNFormatter();
	
	protected BitprintHashURNFormatter() { /* There can be only one! */ }
	
	@Override
	public String format(byte[] hash) {
		return "urn:bitprint:" + BitprintDigest.format(hash);
	}
}
