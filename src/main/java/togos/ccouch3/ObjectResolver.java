package togos.ccouch3;

import togos.blob.ByteBlob;
import togos.ccouch3.rdf.RDFIO;
import togos.ccouch3.rdf.RDFInterpreter;
import togos.ccouch3.rdf.RDFNode;

class ObjectResolver {
	protected final BlobResolver blobResolver;
	public ObjectResolver( BlobResolver blobResolver ) {
		this.blobResolver = blobResolver;
	}
	
	public Object parse( ByteBlob b, String sourceUri ) throws Exception {
		// Assume RDF+XML for now.
		RDFNode n = RDFIO.parseRdf(b, sourceUri);
		return RDFInterpreter.instance.parse(n);
	}
	
	protected static String[] subjectUriPrefixes = {"x-rdf-subject:", "x-parse-rdf:"};
	
	public Object get( String uri ) throws Exception {
		boolean parse = false;
		for( String subjectUriPrefix : subjectUriPrefixes ) {
			if( uri.startsWith(subjectUriPrefix) ) {
				uri = uri.substring(subjectUriPrefix.length());
				parse = true;
			}
		}
		
		ByteBlob b = blobResolver.getBlob(uri);
		return parse ? parse(b, uri) : b;
	}
}