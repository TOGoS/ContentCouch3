package togos.ccouch3.path;

public class Path
{
	/** The path leading here */
	public final PathLink trace;
	/** URN of the current object */
	public final String urn;
	
	public Path( PathLink trace, String urn ) {
		this.trace = trace;
		this.urn = urn;
	}
	
	public String toString(String separator) {
		if( trace != null ) {
			return trace.toString(separator) + " ("+urn+")";
		} else {
			return urn;
		}
	}
	@Override public String toString() { return toString(" > "); }
}
