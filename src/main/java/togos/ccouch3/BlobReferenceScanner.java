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
			"urn:(sha1:[A-Z0-9]{32}|bitprint:[A-Z0-9]{32}\\.[A-Z0-9]{39})",
			Pattern.CASE_INSENSITIVE);
	
	public Pattern urnPattern = SHA1_OR_BITPRINT_PATTERN;
	public boolean reportUnrecursableBlobs = false;
	public boolean reportErrors = true;
	
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
					if( !forEach.handle(matcher.group()) ) {
						success = false;
						if( shortCircuit ) {
							br.close(); // Probably redundant, but makes Eclipse happier.
							return false;
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
	
	public static int main(Iterator<String> argi) {
		BlobReferenceScanner brs = new BlobReferenceScanner();
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
