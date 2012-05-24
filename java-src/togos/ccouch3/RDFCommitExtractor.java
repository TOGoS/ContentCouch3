package togos.ccouch3;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RDFCommitExtractor
{
	static final Pattern PARENT_PATTERN = Pattern.compile( "parent rdf:resource=\"([^\"]+)\"");
	static final Pattern TARGET_PATTERN = Pattern.compile( "target rdf:resource=\"([^\"]+)\"", Pattern.MULTILINE);
	
	/**
	 * Extracts target and parent URNs but ignores everything else.
	 */
	public static Commit getBasicXmlCommitInfo( String commitXml ) {
		ArrayList<String> parentUrns = new ArrayList<String>();
		for( Matcher pmat = PARENT_PATTERN.matcher( commitXml ); pmat.find(); ) {
			parentUrns.add( pmat.group(1) );
		}
		Matcher tmat = TARGET_PATTERN.matcher( commitXml );
		String targetUrn = tmat.find() ? tmat.group(1) : null;
		
		return new Commit( targetUrn, parentUrns.toArray(new String[parentUrns.size()]), new String[0], null, null, -1 );
	}
}
