package togos.ccouch3.rdf;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class RDFNode
{
	static final Map<String,Set<RDFNode>> EMPTY_PROPS = Collections.emptyMap();
	public static final RDFNode EMPTY = new RDFNode(EMPTY_PROPS, null, null, null);
	
	public static RDFNode ref(String uri) {
		return new RDFNode(null, uri, null, null);
	}
	public static RDFNode value(Object val) {
		return new RDFNode(EMPTY_PROPS, null, null, val);
	}
	public static RDFNode typedRef(String typeName, String subjectUri, String sourceUri) {
		return new RDFNode(
			MultiMap.of(RDFNamespace.RDF_TYPE, RDFNode.ref(typeName)),
			null,
			sourceUri,
			null
		);
	}
	
	public final Map<String,Set<RDFNode>> properties;
	public final String subjectUri;
	public final String sourceUri;
	public final Object simpleValue;
	
	protected RDFNode(
		Map<String,Set<RDFNode>> properties,
		String subjectUri, String sourceUri, Object simpleValue
	) {
		this.properties = MultiMap.freeze(properties);
		this.subjectUri = subjectUri;
		this.sourceUri = sourceUri;
		this.simpleValue = simpleValue;
	}
	
	public String getSubjectUri() {
		return subjectUri;
	}
	
	public String getRdfTypeUri() {
		return MultiMap.getSingle(properties, RDFNamespace.RDF_TYPE, EMPTY).subjectUri;
	}
	
	public RDFNode withRdfTypeUri( String typeName ) {
		return new RDFNode(
			MultiMap.withValue(properties, RDFNamespace.RDF_TYPE, RDFNode.ref(typeName)),
			subjectUri, sourceUri, simpleValue
		);
	}
	
	public RDFNode withProperties(Map<String,Set<RDFNode>> properties ) {
		if( properties == this.properties ) return this;
		return new RDFNode(properties, subjectUri, sourceUri, simpleValue);
	}
	
	public RDFNode withAddedProperty(String name, RDFNode value) {
		return withProperties(MultiMap.withValue(properties, name, value));
	}
	
	public RDFNode withSourceUri(String sourceUri) {
		return new RDFNode(properties, subjectUri, sourceUri, simpleValue);
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
	
	@SuppressWarnings("unchecked")
	public Collection<RDFNode> getItems() {
		if( !(simpleValue instanceof Collection) ) {
			throw new RuntimeException("Not a collection");
		}
		return (Collection<RDFNode>)simpleValue;
	}
}
