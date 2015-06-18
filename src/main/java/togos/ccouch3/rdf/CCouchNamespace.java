package togos.ccouch3.rdf;

import java.util.HashMap;
import java.util.Map;

public class CCouchNamespace {
	//// Types //// 
	
	public static final String RDF_SUBJECT_URI_PREFIX = "x-rdf-subject:";
	public static final String RDF_SUBJECT_URI_PREFIX_OLD = "x-parse-rdf:";
	public static final String[] RDF_SUBJECT_URI_PREFIXES = {
		RDF_SUBJECT_URI_PREFIX, RDF_SUBJECT_URI_PREFIX_OLD
	};
	
	public static final String CC_NS = "http://ns.nuke24.net/ContentCouch/";
	public static final String CTX_NS = CC_NS + "Context/";
	public static final String CFG_NS = CC_NS + "Config/";
	
	public static final String NAME             = CC_NS + "name";
	public static final String TAG              = CC_NS + "tag";
	public static final String COLLECTOR        = CC_NS + "collector";
	public static final String IMPORTEDDATE     = CC_NS + "importedDate";
	public static final String IMPORTEDFROM     = CC_NS + "importedFrom";
	public static final String ENTRIES          = CC_NS + "entries";
	/** What kind of object is target? */
	public static final String TARGETTYPE       = CC_NS + "targetType";
	/** Used by both commits and directory entries to indicate their subject. */
	public static final String TARGET           = CC_NS + "target";
	/** Old way to specify file size - use bz:fileLength for new-style things */
	public static final String SIZE             = CC_NS + "size";
	/** If we can't directly represent target, link to its listing */
	public static final String TARGETLISTING    = CC_NS + "targetListing";
	public static final String PARENT           = CC_NS + "parent";

	public static final String HARDLINKABLE     = CC_NS + "hardlinkable";
	public static final String SHA1BASE32       = CC_NS + "sha1Base32";
	public static final String BITPRINT         = CC_NS + "bitprint";
	public static final String PARSED_FROM      = CC_NS + "parsedFrom";

	public static final String BLOB             = CC_NS + "Blob";
	public static final String DIRECTORY        = CC_NS + "Directory";
	public static final String DIRECTORYENTRY   = CC_NS + "DirectoryEntry";
	public static final String COMMIT           = CC_NS + "Commit";
	public static final String REDIRECT         = CC_NS + "Redirect";
	public static final String SOURCE_URI       = CC_NS + "sourceUri";

	//// Object types ////
	
	/*
	 * In old-style RDF, these are used to indicate types of
	 * objects referenced from directory entries using <targetType>.
	 * 
	 * Transitioning to new style, these should be converted
	 * to the fully namespaced versions.  e.g.
	 * 
	 *  <targetType>Blob</targetType> should be interpreted like
	 *  <target><Blob>...</Blob></target>
	 *  
	 *  where <Blob> means thing with <rdf:type rdf:resource="http://ns.nuke24.net/ContentCouch/Blob"/>
	 */
	
	public static final String TT_SHORTHAND_BLOB = "Blob";
	public static final String TT_SHORTHAND_DIRECTORY = "Directory";
	
	public static final String RDF_DIRECTORY_STYLE_NEW = "2";
	public static final String RDF_DIRECTORY_STYLE_OLD = "1";
	
	public static final String ID_SCHEME_DEFAULT = "bitprint";

	//// XML Namespaces ////
	
	static Map<String,String> standardNsAbbreviations = new HashMap<String,String>();
	static {
		standardNsAbbreviations.put("rdf", RDFNamespace.RDF_NS);
		standardNsAbbreviations.put("dc", DCNamespace.DCTERMS_NS);
		standardNsAbbreviations.put("bz", BitziNamespace.BZ_NS);
		standardNsAbbreviations.put("ccouch", CC_NS);
		standardNsAbbreviations.put("xmlns", "http://www.w3.org/2000/xmlns/");
		
		// Some other common namespaces:
		standardNsAbbreviations.put("xhtml", "http://www.w3.org/1999/xhtml");
		standardNsAbbreviations.put("svg", "http://www.w3.org/2000/svg");
		standardNsAbbreviations.put("xlink", "http://www.w3.org/1999/xlink");
		standardNsAbbreviations.put("foaf", "http://xmlns.com/foaf/0.1/");
	}
	
	static Map<String,String> newStandardNsAbbreviations = new HashMap<String,String>();
	static {
		newStandardNsAbbreviations.put("rdf", RDFNamespace.RDF_NS);
		newStandardNsAbbreviations.put("dcterms", DCNamespace.DCTERMS_NS);
		newStandardNsAbbreviations.put("bz", BitziNamespace.BZ_NS);
		newStandardNsAbbreviations.put("ccouch", CC_NS);
		newStandardNsAbbreviations.put("xmlns", "http://www.w3.org/2000/xmlns/");
		
		// Some other common namespaces:
		newStandardNsAbbreviations.put("xhtml", "http://www.w3.org/1999/xhtml");
		newStandardNsAbbreviations.put("svg", "http://www.w3.org/2000/svg");
		newStandardNsAbbreviations.put("xlink", "http://www.w3.org/1999/xlink");
		newStandardNsAbbreviations.put("foaf", "http://xmlns.com/foaf/0.1/");
	}
}
