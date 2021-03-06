package togos.ccouch3.path;

import togos.ccouch3.FSObjectType;

public class PathLink
{
	public final Path origin;
	public final String linkName;
	public final FSObjectType expectedTargetType;
	public final LinkType linkType;
	
	public PathLink( Path origin, String linkName, LinkType linkType, FSObjectType targetType ) {
		this.origin = origin;
		this.linkName = linkName;
		this.linkType = linkType;
		this.expectedTargetType = targetType;
	}
	
	public String toString(String separator) {
		return origin.toString(separator) + separator + linkName; 
	}
	@Override public String toString() { return toString(" > "); }
}
