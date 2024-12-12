package togos.ccouch3.util;

public class ObjectUtil
{
	private ObjectUtil() { throw new RuntimeException("Don't instantiat me, bro!"); }
	
	public static boolean equals(Object a, Object b) {
		if( a == b ) return true;
		if( a == null ) return false;
		return a.equals(b);
	}
	
	public static int hashCode(Object...items) {
		int hashCode = 1;
		int mult = 7;
		for( Object i : items ) {
			int itemHash = i == null ? 0 : i.hashCode();
			hashCode += mult * itemHash;
			mult *= 7;
		}
		return hashCode;
	}
}
