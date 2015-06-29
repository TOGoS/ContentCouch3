package togos.ccouch3;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import togos.blob.ByteBlob;
import togos.ccouch3.path.LinkType;
import togos.ccouch3.path.ObjectType;
import togos.ccouch3.path.Path;
import togos.ccouch3.path.PathLink;
import togos.ccouch3.rdf.CCouchNamespace;
import togos.ccouch3.rdf.DCNamespace;
import togos.ccouch3.rdf.RDFIO;
import togos.ccouch3.rdf.RDFNamespace;
import togos.ccouch3.rdf.RDFNode;
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
	protected final Repository repo;
	boolean followCommitAncestry = true;
	
	public TreeVerifier( Repository repo ) {
		this.repo = repo;
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
	
	protected void walk( RDFNode node, Path path ) {
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
							Object nameObj = entry.properties.getSingle(CCouchNamespace.NAME, RDFNode.EMPTY).simpleValue;
							if( nameObj == null ) {
								logPathError("Directory entry lacks a name", path);
								continue;
							} else if( !(nameObj instanceof String) ) {
								logPathError("Directory entry name is not a string", path);
								continue;
							}
							String name = (String)nameObj;
							String targetUri = entry.properties.getSingle(CCouchNamespace.TARGET, RDFNode.EMPTY).subjectUri;
							if( targetUri == null ) {
								logPathError("Directory entry lacks a target URI", path);
								continue;
							}
							
							ObjectType targetType = ObjectType.UNKNOWN; // TODO: should guess based on entry.target type
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
							walk( new Path(new PathLink(path, "target", LinkType.COMMIT_TARGET, ObjectType.UNKNOWN), target.getSubjectUri()) );
						}
					}
				} else if( CCouchNamespace.PARENT.equals(propKey) ) {
					for( RDFNode parent : prop.getValue() ) {
						if( !parent.hasOnlySubjectUri() ) {
							logPathError("Commit parent is not a simple reference: "+parent, path);
							anyErrors = true;
						} else {
							if( followCommitAncestry ) {
								walk( new Path(new PathLink(path, "parent", LinkType.COMMIT_PARENT, ObjectType.COMMIT), parent.getSubjectUri()) );
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
	
	protected void walk( Path path ) {
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
			System.err.println("IOException while tryig to parse "+blobUri+": "+e.getMessage());
			anyErrors = true;
			return;
		} catch( ParseException e ) {
			System.err.println("ParseException while tryig to parse "+blobUri+": "+e.getMessage());
			anyErrors = true;
			return;
		}
		
		walk(node, path);
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
		System.exit(main( Arrays.asList(args).iterator() ));
	}
}
