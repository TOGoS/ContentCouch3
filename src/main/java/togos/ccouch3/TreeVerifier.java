package togos.ccouch3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import togos.blob.ByteBlob;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;

/**
 * The idea is to debug stored commits and directory trees,
 * printing out any problems (missing blobs, malformed RDF)
 * encountered.
 *
 * Path reporting:
 *   (x-rdf-subject:SOMECOMMIT) > target (x-rdf-subject:SOMEDIRECTORY) > some-file.txt (urn:sha1:SOMEFILE)  
 * 
 * 
 * @author TOGoS
 */
public class TreeVerifier
{
	/** What does the thing referencing a blob think it represents */
	enum ObjectType { UNKNOWN, COMMIT, DIRECTORY, FILE };
	
	enum LinkTargetDisposition { FILE, PARENT, TARGET };
	
	static class PathLink {
		public final Path origin;
		public final String linkName;
		public final ObjectType targetType;
		public final LinkTargetDisposition targetDisposition;
		
		public PathLink( Path origin, String linkName, ObjectType targetType, LinkTargetDisposition targetDisposition ) {
			this.origin = origin;
			this.linkName = linkName;
			this.targetType = targetType;
			this.targetDisposition = targetDisposition;
		}
		
		public String toString(String separator) {
			return origin.toString(separator) + separator + linkName; 
		}
		@Override public String toString() { return toString(" > "); }
	}
	
	static class Path {
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
		
	protected final Repository repo; 
	
	public TreeVerifier( Repository repo ) {
		this.repo = repo;
	}
	
	boolean anythingMissing = false;
	boolean anyErrors = false;
	
	protected void logPathError( String error, Path path ) {
		System.err.println(error);
		System.err.println("  from "+path);
	}
	
	protected void logMissingBlob( String urn, Path path ) {
		logPathError("Could not find blob "+urn, path);
		anythingMissing = true;
	}
	
	protected void logBlobFetchError( Exception e, String urn, Path path ) {
		logPathError("Error fetching blob "+urn+": "+e.getMessage(), path);
		anyErrors = true;
	}
	
	public ByteBlob verifyBlob( String uri, Path path ) {
		ByteBlob b;
		try {
			b = repo.getBlob(uri);
		} catch( IOException e ) {
			logBlobFetchError(e, uri, path);
			return null;
		}
		if( b == null ) {
			logMissingBlob( uri, path );
			return null;
		}
		// TODO: Verify that it matches its URN
		return b;
	}
	
	public void walk( Path path ) {
		String blobUri;
		boolean interpret;
		String uri = path.urn;
		if( uri.startsWith("x-rdf-subject:") ) {
			blobUri = uri.substring(14);
			interpret = true;
		} else if( uri.startsWith("x-parse-rdf:") ) {
			blobUri = uri.substring(12);
			interpret = true;
		} else {
			blobUri = uri;
			interpret = false;
		}
		ByteBlob b = verifyBlob(blobUri, path);
		if( interpret ) {
			System.err.println("Don't know how to interpret RDF blobs, yet.");
			anyErrors = true;
		}
	}
	
	public void walk( String uri ) {
		walk( new Path(null,uri) );
	}
	
	public boolean allIsWell() {
		return !anyErrors && !anythingMissing;
	}
	
	protected static String USAGE =
		"Usage: ccouch3 verify-tree <URN> <URN> ...\n"+
		"Walk an object tree to find problems";
	
	public static int main( Iterator<String> argi ) throws Exception {
		String homeDir = System.getProperty("user.home");
		if( homeDir == null ) homeDir = ".";
		String repoDir = homeDir + "/.ccouch";
		ArrayList<String> urns = new ArrayList<String>();
		for( ; argi.hasNext(); ) {
			String arg = argi.next();
			if( "-repo".equals(arg) ) {
				repoDir = argi.next();
			} else if( CCouch3Command.isHelpArgument(arg) ) {
				System.out.println( USAGE );
				return 0;
			} else if( !arg.startsWith("-") ) {
				urns.add(arg);
			} else {
				System.err.println( "Error: Unrecognised argument: " + arg );
				System.err.println( USAGE );
				return 1;
			}
		}
		Repository repo = new SHA1FileRepository( new File(repoDir + "/data"), "wat");
		// We shouldn't be writing anything to it.
		// If 'wat' sector shows up, something's gone wrong.
		
		TreeVerifier tv = new TreeVerifier(repo);
		for( String urn : urns ) tv.walk(urn);
		if( !tv.allIsWell() ) {
			System.err.println("Stuff's hosed up.");
			return 1;
		} else {
			return 0;
		}
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit(CmdServer.main( Arrays.asList(args).iterator() ));
	}
}
