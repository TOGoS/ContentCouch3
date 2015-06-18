package togos.ccouch3.rdf;

import java.util.Map;
import java.util.Set;

public class RDFNode
{
	public static final RDFNode EMPTY = new RDFNode();
	
	public static RDFNode ref(String uri) {
		RDFNode node = new RDFNode();
		node.subjectUri = uri;
		return node;
	}
	public static RDFNode value(Object val) {
		RDFNode node = new RDFNode();
		node.simpleValue = val;
		return node;
	}

	
	public MultiMap<String,RDFNode> properties;
	public String subjectUri;
	public String sourceUri;
	public Object simpleValue;
	
	public RDFNode() {
		this.properties = new MultiMap<String,RDFNode>();
	}

	public RDFNode( RDFNode cloneFrom ) {
		this.properties = new MultiMap<String,RDFNode>(cloneFrom.properties);
		this.subjectUri = cloneFrom.subjectUri;
		this.sourceUri = cloneFrom.sourceUri;
		this.simpleValue = cloneFrom.simpleValue;
	}
	
	public RDFNode(String typeName, String subjectUri) {
		this();
		if( typeName != null ) this.setRdfTypeUri( typeName );
		this.subjectUri = subjectUri;
	}

	public String getRdfTypeUri() {
		return properties.getSingle(RDFNamespace.RDF_TYPE, EMPTY).subjectUri;
	}
	
	public void setRdfTypeUri( String typeName ) {
		properties.putSingle(RDFNamespace.RDF_TYPE, RDFNode.ref(typeName));
	}
	
	public String getSubjectUri() {
		return subjectUri;
	}

	
	public boolean hasOnlySubjectUri() {
		return properties.size() == 0 && subjectUri != null && simpleValue == null;
	}
	public boolean hasOnlySimpleValue() {
		return properties.size() == 0 && subjectUri == null && simpleValue != null;
	}
	
	public String toString() {
		String s = "RDFNode {";
		if( subjectUri != null ) s += "\n  subjectUri = "+subjectUri;
		if( sourceUri != null  ) s += "\n sourceUri = "+sourceUri;
		if( simpleValue != null ) s += "\n  simpleValue = "+simpleValue;
		for( Map.Entry<String,Set<RDFNode>> prop : properties.entrySet() ) {
			for( RDFNode v : prop.getValue() ) {
				s += "\n  "+prop.getKey()+" = "+v.toString().replace("\n", "\n  ");
			}
		}
		if( s.length() > 9 ) s += "\n";
		s += "}";
		return s;
	}
}