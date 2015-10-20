package togos.ccouch3.rdf;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import togos.blob.ByteBlob;
import togos.ccouch3.Directory;
import togos.ccouch3.DirectoryEntry;
import togos.ccouch3.FSObjectType;
import togos.ccouch3.util.DateUtil;

public class RDFInterpreter
{
	public static final RDFInterpreter instance = new RDFInterpreter();
	
	public static class RDFStructureException extends Exception {
		private static final long serialVersionUID = 1L;
		public final String sourceUri;
		public RDFStructureException(String message, String sourceUri) {
			super(message);
			this.sourceUri = sourceUri;
		}
	}
	
	protected void ensureSimpleValues(Map.Entry<String,Set<RDFNode>> prop, Class<?> expectedClass)
		throws RDFStructureException
	{
		for( RDFNode n : prop.getValue() ) {
			if( !n.hasOnlySimpleValue() ) {
				throw new RDFStructureException("Expected a simple value for "+prop.getKey(), n.sourceUri);
			}
			if( n.simpleValue != null && !expectedClass.isInstance(n.simpleValue) ) {
				throw new RDFStructureException("Expected "+prop.getKey()+" to be a "+expectedClass.getSimpleName()+
					", but found a "+n.simpleValue.getClass().getSimpleName(), n.sourceUri);
			}
		}
	}
	
	protected void ensureSingleValue(Map.Entry<String,Set<RDFNode>> prop, String sourceUri)
		throws RDFStructureException
	{
		if( prop.getValue().size() != 1 ) {
			throw new RDFStructureException("Expected a single value for "+prop.getKey()+", but there are "+prop.getValue().size(), sourceUri);
		}
	}
	
	protected void ensureSimpleValue(Map.Entry<String,Set<RDFNode>> prop, Class<?> expectedClass, String sourceUri)
		throws RDFStructureException
	{
		ensureSingleValue(prop, sourceUri);
		ensureSimpleValues(prop, expectedClass);
	}
	
	public Object parse( RDFNode node ) throws RDFStructureException {
		// Could check that path.trace's expected target type matches actual
		String typeUri = node.getRdfTypeUri();
		if( CCouchNamespace.DIRECTORY.equals(typeUri) ) {
			Directory dir = new Directory();
			for( Map.Entry<String,Set<RDFNode>> prop : node.properties.entrySet() ) {
				String propKey = prop.getKey();
				if( RDFNamespace.RDF_TYPE.equals(propKey) ) {
					// We know.  Ignore.
				} else if( CCouchNamespace.ENTRIES.equals(propKey) ) {
					for( RDFNode entriesNode : prop.getValue() ) {
						if( !(entriesNode.simpleValue instanceof Collection) ) {
							throw new RDFStructureException("Directory#entries is not a collection", node.sourceUri);
						}
						for( RDFNode entry : entriesNode.getItems() ) {
							//System.err.println(entry.toString());
							Object nameObj = entry.properties.getSingle(CCouchNamespace.NAME, RDFNode.EMPTY).simpleValue;
							if( nameObj == null ) {
								throw new RDFStructureException("Directory entry lacks a name", node.sourceUri);
							} else if( !(nameObj instanceof String) ) {
								throw new RDFStructureException("Directory entry name is not a string", node.sourceUri);
							}
							String name = (String)nameObj;
							RDFNode target = entry.properties.getSingle(CCouchNamespace.TARGET, RDFNode.EMPTY);
							String targetUri = target.subjectUri;
							if( targetUri == null ) {
								throw new RDFStructureException("Directory entry lacks a target URI", node.sourceUri);
							}
							RDFNode sizeNode = target.properties.getSingle(BitziNamespace.BZ_FILELENGTH, null);
							long size = sizeNode == null ? -1 : Long.parseLong(sizeNode.simpleValue.toString());
							RDFNode mtimeNode = target.properties.getSingle(DCNamespace.DC_MODIFIED, null);
							long mtime;
							try {
								mtime = mtimeNode == null ? -1 : DateUtil.parseDate(mtimeNode.simpleValue.toString()).getTime();
							} catch (ParseException e) {
								throw new RDFStructureException("Failed to parse date: "+mtimeNode.simpleValue.toString(), node.sourceUri);
							}
							
							FSObjectType targetType = FSObjectType.UNKNOWN; // TODO: should guess based on entry.target type
							dir.add(new DirectoryEntry(name, targetUri, targetType, size, mtime));
						}
					}
				} else {
					throw new RDFStructureException("Don't know what to do with Directory property: "+prop.getKey(), node.sourceUri);
				}
			}
			return dir;
		} else if( CCouchNamespace.COMMIT.equals(typeUri) ) {
			int targetCount = 0;
			String targetUri = null;
			for( Map.Entry<String,Set<RDFNode>> prop : node.properties.entrySet() ) {
				String propKey = prop.getKey();
				if( RDFNamespace.RDF_TYPE.equals(propKey) ) {
					// We know.  Ignore.
				} else if( CCouchNamespace.TARGET.equals(propKey) ) {
					targetCount += prop.getValue().size();
					for( RDFNode target : prop.getValue() ) {
						if( !target.hasOnlySubjectUri() ) {
							throw new RDFStructureException("Commit target is not a simple reference: "+target, node.sourceUri);
						}
						targetUri = target.getSubjectUri();
					}
				} else if( CCouchNamespace.PARENT.equals(propKey) ) {
					for( RDFNode parent : prop.getValue() ) {
						if( !parent.hasOnlySubjectUri() ) {
							throw new RDFStructureException("Commit parent is not a simple reference: "+parent, node.sourceUri);
						}
						// todo: stuff
					}
				} else if( DCNamespace.DC_CREATOR.equals(propKey) ) {
					ensureSimpleValues(prop, String.class);
				} else if( DCNamespace.DC_CREATED.equals(propKey) ) {
					ensureSimpleValue(prop, String.class, node.sourceUri);
				} else if( DCNamespace.DC_DESCRIPTION.equals(propKey) ) {
					ensureSimpleValue(prop, String.class, node.sourceUri);
				} else {
					throw new RDFStructureException("Don't know what to do with Commit property: "+prop.getKey(), node.sourceUri);
				}
			}
			if( targetCount != 1 ) {
				throw new RDFStructureException("Commit should have exactly 1 target, but this one has "+targetCount+".", node.sourceUri);
			}
			throw new UnsupportedOperationException("Haha j/k I don't parse commits yet");
			// todo: stuff
		} else {
			throw new RDFStructureException("Don't know what to do with a "+typeUri, node.sourceUri);
		}
	}
	
	public Object parse( ByteBlob b, String sourceUri ) throws IOException, ParseException, RDFStructureException {
		return parse(RDFIO.parseRdf(b, sourceUri));
	}
}
