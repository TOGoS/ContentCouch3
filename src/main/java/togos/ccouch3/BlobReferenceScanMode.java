package togos.ccouch3;

import java.io.PrintStream;
import java.sql.Blob;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Indicates a method for scanning blobs for references to other blobs,
 * e.g. to recursively download them.
 * 
 * Note that the names of these are used to find some SLF cache files,
 * so don't go changing them around willy-nilly.
 */
enum BlobReferenceScanMode {
	NEVER("never", "Never scan blobs"),
	SCAN_TEXT_FOR_URNS("text", "Scan UTF-8 text blobs for 'urn:....'"),
	SCAN_TEXT_FOR_RDF_OBJECT_URNS("text-rdf", "Scan UTF-8 text blobs for 'parse-rdf:' or 'x-rdf-subject:'-prefixed or '#'-postfixed URNs");

	public String getNiceName() {
		return this.name().replaceAll("_", "-").toLowerCase();
	}

	public final String cacheDbName;
	public final String description;
	BlobReferenceScanMode(String cacheDbName, String description) {
		this.cacheDbName = cacheDbName;
		this.description = description;
	}

	public static BlobReferenceScanMode forNiceName(String name) {
		return BlobReferenceScanMode.valueOf(name.toUpperCase().replaceAll("-", "_"));
	}

	static final Pattern recurseArgPattern = Pattern.compile("^-recurse:(.*)");

		public static BlobReferenceScanMode parseRecurseArg(String arg) {
		if( "-recurse".equals(arg) ) return SCAN_TEXT_FOR_URNS;
		Matcher m = recurseArgPattern.matcher(arg);
		if( m.matches() ) {
			String scanModeStr = m.group(1);
			if( "?".equals(scanModeStr) ) {
				dumpModes(System.out);
				System.exit(0);
			}
			try {
				return forNiceName(scanModeStr);
			} catch( IllegalArgumentException e ) {
				System.err.println("Error: invalid -recurse mode: '" + scanModeStr + "'");
				dumpModes(System.err);
				System.exit(1);
			}
		}
		return null;
	}

	public static void dumpModes(PrintStream ps) {
		ps.println("Scan modes:");
		for( BlobReferenceScanMode mode : BlobReferenceScanMode.values() ) {
			ps.println("  "+mode.getNiceName()+" ; " + mode.description);
		}
	}
}
