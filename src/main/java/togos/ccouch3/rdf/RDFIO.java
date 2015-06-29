package togos.ccouch3.rdf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import togos.blob.ByteBlob;
import togos.ccouch3.xml.XML;
import togos.ccouch3.xml.XML.XMLCloseTag;
import togos.ccouch3.xml.XML.XMLOpenTag;

public class RDFIO {
	public static void writeRdfValue( Appendable w, RDFNode desc, String padding, Map<String,String> standardNsAbbreviations, Map<String,String> usedNsAbbreviations )
		throws IOException
	{
		String valueNodeName = XML.longToShort(desc.getRdfTypeUri(), standardNsAbbreviations, usedNsAbbreviations );
		int wpCount = 0;
		for( Iterator<String> propIter = desc.properties.keySet().iterator(); propIter.hasNext(); ) {
			String propName = (String)propIter.next();
			if( !RDFNamespace.RDF_TYPE.equals(propName) ) ++wpCount;
		}
		String subUri = desc.getSubjectUri();
		String subUriAttrStr;
		if( subUri != null ) {
			subUriAttrStr = " "+XML.longToShort(RDFNamespace.RDF_ABOUT, standardNsAbbreviations, usedNsAbbreviations)+"=\""+XML.xmlEscapeAttributeValue(subUri)+"\"";
		} else {
			subUriAttrStr = "";
		}
		if( wpCount > 0 ) {
			w.append(padding + "<" + valueNodeName + subUriAttrStr + ">\n");
			writeRdfProperties( w, desc.properties, padding + "\t", standardNsAbbreviations, usedNsAbbreviations);
			w.append(padding + "</" + valueNodeName + ">");
		} else {
			w.append(padding + "<" + valueNodeName + subUriAttrStr + "/>");
		}
	}
	
	/** write a single property, not including trailing newline */
	public static void writeRdfProperty( Appendable w, String propName, RDFNode value, String padding, Map<String,String> standardNsAbbreviations, Map<String,String> usedNsAbbreviations )
		throws IOException
	{
		String propNodeName = XML.longToShort(propName, CCouchNamespace.standardNsAbbreviations, usedNsAbbreviations);
		
		if( value == null ) {
			// Then we just skip it!
		} else if( value.hasOnlySubjectUri() ) {
			w.append(padding + "<" + propNodeName + " rdf:resource=\"" + XML.xmlEscapeAttributeValue(value.subjectUri) + "\"/>");
		} else if( value.hasOnlySimpleValue() ) {
			if( value.simpleValue instanceof Collection ) {
				w.append(padding + "<" + propNodeName + " rdf:parseType=\"Collection\"");
				Collection<RDFNode> c = value.getItems();
				// In old style, empty list -> \t<List rdf:parseType="Collection">\n\t</List>
				// In new style, empty list -> \t<List rdf:parseType="Collection"/>
				if( c.size() > 0 ) {
					w.append(">\n");
					for( Iterator<RDFNode> i = c.iterator(); i.hasNext(); ) {
						writeRdfValue( w, i.next(), padding + "\t", standardNsAbbreviations, usedNsAbbreviations );
						w.append("\n");
					}
					w.append(padding + "</" + propNodeName + ">");
				} else {
					w.append("/>");
				}
			} else {
				w.append(padding + "<" + propNodeName + ">" + XML.xmlEscapeText(value.simpleValue.toString()) + "</" + propNodeName + ">");
			}
		} else {
			w.append(padding + "<" + propNodeName + ">\n");
			writeRdfValue( w, value, padding + "\t", standardNsAbbreviations, usedNsAbbreviations );
			w.append("\n" + padding + "</" + propNodeName + ">");
		}
	}
	
	/** write all properties.  trailing newline included */
	public static void writeRdfProperties( Appendable w, String propName, Collection<RDFNode> values, String padding, Map<String,String> standardNsAbbreviations, Map<String,String> usedNsAbbreviations )
		throws IOException
	{
		for( Iterator<RDFNode> i = values.iterator(); i.hasNext(); ) {
			writeRdfProperty( w, propName, i.next(), padding, standardNsAbbreviations, usedNsAbbreviations );
			w.append("\n");
		}
	}
	
	public static <T extends Comparable<? super T>> List<T> sort(Collection<T> stuff) {
		List<T> sortedStuff = new ArrayList<T>(stuff);
		Collections.sort(sortedStuff);
		return sortedStuff;
	}
	
	public static void writeRdfProperties( Appendable w, MultiMap<String,RDFNode> properties, String padding, Map<String,String> standardNsAbbreviations, Map<String,String> usedNsAbbreviations )
		throws IOException
	{
		for( Iterator<String> propIter = sort(properties.keySet()).iterator(); propIter.hasNext(); ) {
			String propName = propIter.next();
			if( RDFNamespace.RDF_TYPE.equals(propName) ) continue;
			Set<RDFNode> values = properties.getSet(propName);
			
			writeRdfProperties( w, propName, values, padding, standardNsAbbreviations, usedNsAbbreviations );
		}
	}
	
	public static String xmlEncodeRdf( Object value, String defaultNamespace, Map<String,String> standardNsAbbreviations ) {
		try {
			if( value instanceof RDFNode ) {
				RDFNode desc = (RDFNode)value;
	
				Appendable subWriter = new StringWriter();
				Map<String,String> usedNsAbbreviations = new HashMap<String,String>();
				usedNsAbbreviations.put("rdf", standardNsAbbreviations.get("rdf"));
				if( defaultNamespace != null ) {
					usedNsAbbreviations.put("", defaultNamespace);
				}
				writeRdfProperties( subWriter, desc.properties, "\t", standardNsAbbreviations, usedNsAbbreviations );
				
				String typeUri = desc.getRdfTypeUri();
				if( typeUri == null ) typeUri = RDFNamespace.RDF_DESCRIPTION;
				String nodeName = XML.longToShort(typeUri, CCouchNamespace.standardNsAbbreviations, usedNsAbbreviations);
				Appendable outerWriter = new StringWriter();
				outerWriter.append( "<" + nodeName );
				XML.writeXmlns( outerWriter, usedNsAbbreviations );
				if( desc.getSubjectUri() != null ) {
					outerWriter.append(" rdf:about=\"" + XML.xmlEscapeAttributeValue(desc.getSubjectUri()) + "\"");
				}
				String nodely = subWriter.toString();
				if( nodely.length() > 0 ) {
					outerWriter.append( ">\n" );
					outerWriter.append( nodely );
					outerWriter.append( "</" + nodeName + ">" );
				} else {
					outerWriter.append( "/>" );
				}
				return outerWriter.toString();
			} else {
				return XML.xmlEscapeText(value.toString());
			}
		} catch( IOException e ) {
			throw new RuntimeException( "Error while generating xml", e );
		}
	}
	
	public static String xmlEncodeRdf( Object value ) {
		return xmlEncodeRdf( value, null, CCouchNamespace.newStandardNsAbbreviations );
	}
	
	//// RDF Parsing ////
	
	static class RDFXMLParseException extends ParseException
	{
		private static final long serialVersionUID = 1L;
		
		public RDFXMLParseException(String message, int offset) {
			super(message, offset);
		}
	}
	
	public static XML.ParseResult parseRdf( char[] chars, int offset, Map<String,String> nsAbbreviations, String sourceUri ) throws ParseException {
		XML.ParseResult xmlParseResult = XML.parseXmlPart(chars, offset);
		Object xmlPart = xmlParseResult.value;
		
		if( xmlPart instanceof XMLOpenTag ) {
			offset = xmlParseResult.newOffset;
			
			XMLOpenTag descOpenTag = (XMLOpenTag)xmlPart;
			Map<String,String> descNsAbbreviatios = XML.updateNamespaces( descOpenTag, nsAbbreviations );
			descOpenTag = (XMLOpenTag)XML.namespaceXmlPart(descOpenTag, descNsAbbreviatios);

			// TODO:
			// Always return regular RdfNodes
			// Subject should not _be_ the RDF node, but gotten by
			// Calling RdfInterpreter#interpretSubject
			String className = RDFNamespace.RDF_DESCRIPTION.equals(descOpenTag.name) ? null : descOpenTag.name;
			RDFNode desc = new RDFNode(className, descOpenTag.attributes.get(RDFNamespace.RDF_ABOUT));
			desc.sourceUri = sourceUri;
			
			if( descOpenTag.closed ) {
				return new XML.ParseResult( desc, offset );
			}
			
			while( true ) {
				xmlParseResult = XML.parseXmlPart(chars, offset);
				xmlPart = xmlParseResult.value;
				Map<String,String> predicateNsAbbreviations = XML.updateNamespaces(xmlPart, descNsAbbreviatios);
				xmlPart = XML.namespaceXmlPart(xmlPart, predicateNsAbbreviations);
				
				if( xmlPart instanceof XMLCloseTag ) {
					if( !descOpenTag.name.equals(((XMLCloseTag)xmlPart).name) ) {
						throw new RDFXMLParseException("Start and end tags do not match: " + descOpenTag.name + " != " + ((XMLCloseTag)xmlPart).name, offset);
					}
					return new XML.ParseResult( desc, xmlParseResult.newOffset );
				} else if( xmlPart instanceof XMLOpenTag ) {
					XMLOpenTag predicateOpenTag = (XMLOpenTag)xmlPart;
					offset = xmlParseResult.newOffset;
					
					String resourceUri = (String)predicateOpenTag.attributes.get(RDFNamespace.RDF_RESOURCE);
					if( resourceUri != null ) {
						desc.properties.add(predicateOpenTag.name, RDFNode.ref(resourceUri));
					}
					
					if( !predicateOpenTag.closed ) {
						Collection<RDFNode> c = null;
						if( "Collection".equals(predicateOpenTag.attributes.get(RDFNamespace.RDF_PARSETYPE)) ) {
							c = new ArrayList<RDFNode>();
							desc.properties.add(predicateOpenTag.name, RDFNode.value(c));
						}
						while( true ) {
							XML.ParseResult rdfValueParseResult = parseRdf(chars, offset, predicateNsAbbreviations, sourceUri);
							offset = rdfValueParseResult.newOffset;
							if( rdfValueParseResult.value == null ) {
								break;
							}
							RDFNode value;
							if( rdfValueParseResult.value instanceof RDFNode ) {
								value = (RDFNode)rdfValueParseResult.value;
							} else if( rdfValueParseResult.value instanceof String ) {
								value = RDFNode.value(rdfValueParseResult.value);
							} else {
								throw new RuntimeException("Unexpected rdfValueParseResult.value: "+rdfValueParseResult.value);
							}
							if( c != null ) {
								c.add(value);
							} else {
								desc.properties.add(predicateOpenTag.name, value);
							}
						}
						XML.ParseResult predicateCloseTagParseResult = XML.parseXmlPart(chars, offset);
						if( !(predicateCloseTagParseResult.value instanceof XMLCloseTag) ) {
							throw new RDFXMLParseException("Expected XML close tag but found " + predicateCloseTagParseResult.value.getClass().getName(), offset );
						}
						XMLCloseTag predicateCloseTag = (XMLCloseTag)predicateCloseTagParseResult.value;
						predicateCloseTag = (XMLCloseTag)XML.namespaceXmlPart(predicateCloseTag, predicateNsAbbreviations);
						if( !predicateCloseTag.name.equals(predicateOpenTag.name) ) {
							throw new RDFXMLParseException("Start and end predicate tags do not match: " + predicateOpenTag.name + " != " + predicateCloseTag.name, offset );
						}
						offset = predicateCloseTagParseResult.newOffset;
					}
				} else {
					offset = xmlParseResult.newOffset;
					// somehow report unrecognised element?
				}
			}
		} else if( xmlPart instanceof XMLCloseTag ) {
			return new XML.ParseResult( null, offset );
		} else {
			return new XML.ParseResult( xmlPart, xmlParseResult.newOffset );
		}
	}
	
	public static RDFNode parseRdf( String rdf, String sourceUri ) throws ParseException {
		char[] chars = new char[rdf.length()];
		rdf.getChars(0, chars.length, chars, 0);
		Map<String,String> nsAbbreviations = CCouchNamespace.standardNsAbbreviations;
		XML.ParseResult rdfParseResult = parseRdf( chars, 0, nsAbbreviations, sourceUri );
		if( rdfParseResult.value instanceof RDFNode ) {
			((RDFNode)rdfParseResult.value).sourceUri = sourceUri;
		} else {
			throw new RDFXMLParseException("RDF parse result wasn't an RDFNode, but a "+rdfParseResult.value.getClass().getName(), 0);
		}
		return (RDFNode)rdfParseResult.value;
	}
	
	public static RDFNode parseRdf( ByteBlob rdf, String sourceUri ) throws IOException, ParseException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		InputStream is = rdf.openInputStream();
		byte[] buffer = new byte[65536];
		int z;
		while( (z = is.read(buffer)) > 0 ) {
			baos.write(buffer, 0, z);
		}
		return parseRdf( new String(baos.toByteArray(), "UTF-8"), sourceUri );
	}
}
