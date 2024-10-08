package togos.ccouch3;

import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import togos.blob.ByteBlob;
import togos.ccouch3.path.LinkType;
import togos.ccouch3.path.Path;
import togos.ccouch3.path.PathLink;
import togos.ccouch3.rdf.CCouchNamespace;
import togos.ccouch3.rdf.DCNamespace;
import togos.ccouch3.rdf.MultiMap;
import togos.ccouch3.rdf.RDFIO;
import togos.ccouch3.rdf.RDFNamespace;
import togos.ccouch3.rdf.RDFNode;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.util.ListUtil;
import togos.ccouch3.util.ParseResult;

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
	public static interface PathCallback {
		public void pathVisited( Path p ) throws Exception;
	}
	
	public static class NoopPathCallback implements PathCallback {
		public static final NoopPathCallback INSTANCE = new NoopPathCallback();
		private NoopPathCallback() { }
		@Override
		public void pathVisited( Path p ) { }
	}
	
	protected final Repository repo;
	protected final PathCallback pathCallback;
	boolean followCommitAncestry = true;
	
	public TreeVerifier( Repository repo, PathCallback callback ) {
		this.repo = repo;
		this.pathCallback = callback;
	}
	
	public TreeVerifier( Repository repo ) {
		this( repo, NoopPathCallback.INSTANCE );
	}
	
	boolean anythingMissing = false;
	boolean anyErrors = false;
	
	protected void logPathError( String error, Path path ) {
		System.err.println(error);
		System.err.println("  in "+path.toString("\n   > "));
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
	
	protected void ensureSimpleValues(Map.Entry<String,Set<RDFNode>> prop, Class<?> expectedClass, Path path) {
		for( RDFNode n : prop.getValue() ) {
			if( !n.hasOnlySimpleValue() ) {
				logPathError("Expected a simple value for "+prop.getKey(), path);
				anyErrors = true;
			}
			if( n.simpleValue != null && !expectedClass.isInstance(n.simpleValue) ) {
				logPathError("Expected "+prop.getKey()+" to be a "+expectedClass.getSimpleName()+
					", but found a "+n.simpleValue.getClass().getSimpleName(), path);
			}
		}
	}
	
	protected void ensureSingleValue(Map.Entry<String,Set<RDFNode>> prop, Path path) {
		if( prop.getValue().size() != 1 ) {
			logPathError("Expected a single value for "+prop.getKey()+", but there are "+prop.getValue().size(), path);
			anyErrors = true;
		}
	}
	
	protected void ensureSimpleValue(Map.Entry<String,Set<RDFNode>> prop, Class<?> expectedClass, Path path) {
		ensureSingleValue(prop, path);
		ensureSimpleValues(prop, expectedClass, path);
	}
	
	protected void walk( RDFNode node, Path path ) throws Exception {
		// Could check that path.trace's expected target type matches actual
		String typeUri = node.getRdfTypeUri();
		if( CCouchNamespace.DIRECTORY.equals(typeUri) ) {
			for( Map.Entry<String,Set<RDFNode>> prop : node.properties.entrySet() ) {
				String propKey = prop.getKey();
				if( RDFNamespace.RDF_TYPE.equals(propKey) ) {
					// We know.  Ignore.
				} else if( CCouchNamespace.ENTRIES.equals(propKey) ) {
					for( RDFNode entriesNode : prop.getValue() ) {
						if( !(entriesNode.simpleValue instanceof Collection) ) {
							logPathError("Directory#entries is not a collection", path);
							anyErrors = true;
							continue;
						}
						for( RDFNode entry : entriesNode.getItems() ) {
							//System.err.println(entry.toString());
							Object nameObj = MultiMap.getSingle(entry.properties, CCouchNamespace.NAME, RDFNode.EMPTY).simpleValue;
							if( nameObj == null ) {
								logPathError("Directory entry lacks a name", path);
								continue;
							} else if( !(nameObj instanceof String) ) {
								logPathError("Directory entry name is not a string", path);
								continue;
							}
							String name = (String)nameObj;
							String targetUri = MultiMap.getSingle(entry.properties, CCouchNamespace.TARGET, RDFNode.EMPTY).subjectUri;
							if( targetUri == null ) {
								logPathError("Directory entry lacks a target URI", path);
								continue;
							}
							
							FSObjectType targetType = FSObjectType.UNKNOWN; // TODO: should guess based on entry.target type
							walk( new Path(new PathLink(path, name, LinkType.DIRECTORY_ENTRY, targetType), targetUri) );
						}
					}
				} else {
					logPathError("Don't know what to do with Directory property: "+prop.getKey(), path);
					anyErrors = true;
				}
			}
		} else if( CCouchNamespace.COMMIT.equals(typeUri) ) {
			int targetCount = 0;
			for( Map.Entry<String,Set<RDFNode>> prop : node.properties.entrySet() ) {
				String propKey = prop.getKey();
				if( RDFNamespace.RDF_TYPE.equals(propKey) ) {
					// We know.  Ignore.
				} else if( CCouchNamespace.TARGET.equals(propKey) ) {
					targetCount += prop.getValue().size();
					for( RDFNode target : prop.getValue() ) {
						if( !target.hasOnlySubjectUri() ) {
							logPathError("Commit target is not a simple reference: "+target, path);
							anyErrors = true;
						} else {
							walk( new Path(new PathLink(path, "target", LinkType.COMMIT_TARGET, FSObjectType.UNKNOWN), target.getSubjectUri()) );
						}
					}
				} else if( CCouchNamespace.PARENT.equals(propKey) ) {
					for( RDFNode parent : prop.getValue() ) {
						if( !parent.hasOnlySubjectUri() ) {
							logPathError("Commit parent is not a simple reference: "+parent, path);
							anyErrors = true;
						} else {
							if( followCommitAncestry ) {
								walk( new Path(new PathLink(path, "parent", LinkType.COMMIT_PARENT, FSObjectType.COMMIT), parent.getSubjectUri()) );
							}
						}
					}
				} else if( DCNamespace.DC_CREATOR.equals(propKey) ) {
					ensureSimpleValues(prop, String.class, path);
				} else if( DCNamespace.DC_CREATED.equals(propKey) ) {
					ensureSimpleValue(prop, String.class, path);
				} else if( DCNamespace.DC_DESCRIPTION.equals(propKey) ) {
					ensureSimpleValue(prop, String.class, path);
				} else {
					logPathError("Don't know what to do with Commit property: "+prop.getKey(), path);
					anyErrors = true;
				}
			}
			if( targetCount != 1 ) {
				logPathError("Commit should have exactly 1 target, but this one has "+targetCount+".", path);
				anyErrors = true;
			}
		} else {
			logPathError("Don't know what to do with a "+typeUri, path);
			anyErrors = true;
		}
	}
	
	protected void walk( Path path ) throws Exception {
		this.pathCallback.pathVisited(path);
		
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
		if( !interpret || b == null ) return;
		
		RDFNode node;
		try {
			node = RDFIO.parseRdf(b, blobUri);
		} catch( IOException e ) {
			System.err.println("IOException while trying to parse "+blobUri+": "+e.getMessage());
			anyErrors = true;
			return;
		} catch( ParseException e ) {
			System.err.println("ParseException while trying to parse "+blobUri+": "+e.getMessage());
			anyErrors = true;
			return;
		}
		
		walk(node, path);
	}
	
	public void walk( String uri ) throws Exception {
		walk( new Path(null,uri) );
	}
	
	public boolean allIsWell() {
		return !anyErrors && !anythingMissing;
	}
	
	protected static String USAGE =
		"Usage: ccouch3 verify-tree <URN> <URN> ...\n"+
		"Walk an object tree to find problems";
	
	enum OutputMode {
		SILENT,
		PATH_TO_URN,
		URN_LIST
	}
	
	protected static PathCallback pathCallback( OutputMode om, final PrintStream dest ) {
		switch( om ) {
		case SILENT:
			return NoopPathCallback.INSTANCE;
		case PATH_TO_URN:
			return new PathCallback() {
				protected String abbreviate(String originUrn) {
					return originUrn.length() > 8 ?
						"..."+originUrn.substring(originUrn.length()-5) :
						originUrn;
				}
				
				protected String pathStr(Path l) {
					return l.trace == null ?
						abbreviate(l.urn) :
						pathStr(l.trace.origin) + "/" + l.trace.linkName;
				}
				
				@Override
				public void pathVisited(Path p) throws IOException {
					dest.println(pathStr(p)+"\t"+p.urn);
				}
			};
		default:
			throw new RuntimeException("Idk "+om);
		}
	}
	
	public static int main(CCouchContext ctx, List<String> args ) throws Exception {
		ArrayList<String> urns = new ArrayList<String>();
		OutputMode outputMode = OutputMode.SILENT;
		while( !args.isEmpty() ) {
			ParseResult<List<String>,CCouchContext> ctxPr = ctx.handleCommandLineOption(args);
			if( ctxPr.remainingInput != args ) {
				args = ctxPr.remainingInput;
				ctx  = ctxPr.result;
				continue;
			}
			
			String arg = ListUtil.car(args);
			args = ListUtil.cdr(args);
			if( "-v".equals(arg) ) {
				outputMode = OutputMode.PATH_TO_URN;
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
		Repository repo = ctx.getPrimaryRepository();
		// We shouldn't be writing anything to it.
		// If 'wat' sector shows up, something's gone wrong.
		
		TreeVerifier tv = new TreeVerifier(repo, pathCallback(outputMode, System.out));
		for( String urn : urns ) tv.walk(urn);
		if( !tv.allIsWell() ) {
			System.err.println("Stuff's hosed up.");
			return 1;
		} else {
			return 0;
		}
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit(main( new CCouchContext(), Arrays.asList(args) ));
	}
}
