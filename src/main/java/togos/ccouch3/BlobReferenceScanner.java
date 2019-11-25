package togos.ccouch3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import togos.ccouch3.util.Charsets;

public class BlobReferenceScanner
{
	interface ScanCallback {
		public boolean handle(String t);
	}

	static final Pattern SHA1_OR_BITPRINT_PATTERN = Pattern.compile(
			"(urn:(?:sha1:[A-Z0-9]{32}|bitprint:[A-Z0-9]{32}\\.[A-Z0-9]{39}))",
			Pattern.CASE_INSENSITIVE);
	static final Pattern RDF_SUBJECT_SHA1_OR_BITPRINT_PATTERN = Pattern.compile(
			"(?:parse-rdf:|rdf-subject:)"+SHA1_OR_BITPRINT_PATTERN.pattern()+"|"+
			SHA1_OR_BITPRINT_PATTERN.pattern()+"#",
			Pattern.CASE_INSENSITIVE);

	public Pattern urnPattern = SHA1_OR_BITPRINT_PATTERN;
	public boolean reportUnrecursableBlobs = false;
	public boolean reportErrors = true;

	protected static Pattern patternForScanMode(BlobReferenceScanMode mode) {
		switch( mode ) {
		case NEVER: throw new RuntimeException("Not scanning; no pattern applies");
		case SCAN_TEXT_FOR_URNS: return SHA1_OR_BITPRINT_PATTERN;
		case SCAN_TEXT_FOR_RDF_OBJECT_URNS: return RDF_SUBJECT_SHA1_OR_BITPRINT_PATTERN;
		default: throw new RuntimeException("Bad blob reference scan mode: "+mode);
		}
	};

	/**
	 * @param urnPattern Pattern to search for; group(1) will be passed to forEach.
	 */
	protected BlobReferenceScanner(Pattern urnPattern) {
		this.urnPattern = urnPattern;
	}
	public BlobReferenceScanner(BlobReferenceScanMode scanMode) {
		this(patternForScanMode(scanMode));
	}

	/**
	 * Scan the provided InputStream for URNs, calling forEach for each one found.
	 * It will return true only if the blob identified by 'urn' is successfully found
	 * and scanned and forEach returns true for all embedded URNs.
	 * If shortCircuit is true, this will return as soon as forEach returns false.
	 */
	public boolean scanTextForUrns( String name, InputStream is, BlobReferenceScanner.ScanCallback forEach, boolean shortCircuit ) {
		boolean success = true;
		try {
			CharsetDecoder utf8decoder = Charsets.UTF8.newDecoder();
			// Have it throw CharacterCodingException on non-UTF-8 input!
			utf8decoder.onMalformedInput(CodingErrorAction.REPORT);
			
			BufferedReader br = new BufferedReader(new InputStreamReader(is, utf8decoder));
			String line;
			Matcher matcher = urnPattern.matcher("");
			while( (line = br.readLine()) != null ) {
				matcher.reset(line);
				while( matcher.find() ) {
					for( int g=1; g<=matcher.groupCount(); ++g ) {
						String urn = matcher.group(g);
						if( urn == null ) continue;
						if (!forEach.handle(matcher.group(g))) {
							success = false;
							if (shortCircuit) {
								return false;
							}
						}
					}
				}
			}
		} catch( CharacterCodingException e ) {
			// This is normal and counts as success!
			if( reportUnrecursableBlobs ) {
				System.err.println("Not valid UTF-8; can't scan: "+name);
			}
		} catch( IOException e ) {
			// This is not, and doesn't.
			success = false;
			if( reportErrors ) {
				System.err.println("Error while scanning for URNs in "+name+" due to an "+e.getClass().getName());
				e.printStackTrace();
			}
		} finally {
			try {
				is.close();
			} catch( IOException e ) {
				if( reportErrors ) {
					System.err.println("Failed to close InputStream of "+name+" after scanning for URNs");
				}
			}
		}
		return success;
	}

	public static String USAGE =
			"Usage: ccouch3 extract-urns [-mode {scan-text-for-urns|scan-text-for-rdf-object-urns}]";
	
	public static int main(Iterator<String> argi) {
		BlobReferenceScanMode scanMode = BlobReferenceScanMode.SCAN_TEXT_FOR_URNS;
		while( argi.hasNext() ) {
			String arg = argi.next();
			if( "-mode".equals(arg) ) {
				String scanModeStr = argi.next();
				if( scanModeStr == null ) {
					System.err.println("Error: -mode requires an argument");
					return 1;
				}
				String scanModeName = scanModeStr.replaceAll("-", "_" ).toUpperCase();
				try {
					scanMode = BlobReferenceScanMode.valueOf(scanModeName);
				} catch( IllegalArgumentException e ) {
					System.err.println("Error: invalid -mode: '"+scanModeStr+"'");
					BlobReferenceScanMode.dumpModes(System.err);
					return 1;
				}
			} else if( CCouch3Command.isHelpArgument(arg) ) {
				System.out.println(USAGE);
				return 0;
			} else {
				System.err.println("Error: Unrecognized argument: '"+arg+"'");
				return 1;
			}
		}
		BlobReferenceScanner brs = new BlobReferenceScanner(scanMode);
		String inputStreamName = "stdin";
		InputStream is = System.in;
		brs.scanTextForUrns(inputStreamName, is, new ScanCallback() {
			@Override public boolean handle(String t) {
				System.out.println(t);
				return true;
			}
		}, false);
		return 0;
	}
}
