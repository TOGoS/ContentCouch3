package togos.ccouch3;

import junit.framework.TestCase;

public class RDFCommitExtractorTest extends TestCase
{
	protected static final String SOME_COMMIT_2_XML =
		"<Commit xmlns=\"http://ns.nuke24.net/ContentCouch/\" xmlns:dc=\"http://purl.org/dc/terms/\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
		"\t<parent rdf:resource=\"x-rdf-subject:urn:bitprint:35OQJVDODQ4ZRCVD4YS2ZIFLTHHZGT22.QYHXX6XLB6JYAX7P46UKH2VHMOWCXWB4LJV2UGY\"/>\n" +
		"\t<parent rdf:resource=\"x-rdf-subject:urn:bitprint:75OQJVDODQ4ZRCVD4YS2ZIFLTHHZGT22.QYHXX6XLB6JYAX7P46UKH2VHMOWCXWB4LJV2UGY\"/>\n" +
		"\t<parent rdf:resource=\"x-rdf-subject:urn:bitprint:85OQJVDODQ4ZRCVD4YS2ZIFLTHHZGT22.QYHXX6XLB6JYAX7P46UKH2VHMOWCXWB4LJV2UGY\"/>\n" +
		"\t<tag>bar</tag>\n" +
		"\t<tag>baz</tag>\n" +
		"\t<tag>foo</tag>\n" +
		"\t<target rdf:resource=\"x-rdf-subject:urn:bitprint:QWKZDK5UAJ4KOEYMK3VCMPUYZBZBUL7C.PJHNDGTCF26WPTLSNKXNBZDBGCHILX3YNHDAUXY\"/>\n" +
		"\t<dc:created>2012-05-10 05:28:42 GMT</dc:created>\n" +
		"\t<dc:creator>maude-trtd</dc:creator>\n" +
		"\t<dc:description>TOGoS image archive on Maude, 2012-05-10 00:28:33</dc:description>\n" +
		"</Commit>\n";

	public void testExtract() {
		Commit c = RDFCommitExtractor.getBasicXmlCommitInfo( SOME_COMMIT_2_XML );
		assertEquals( "x-rdf-subject:urn:bitprint:QWKZDK5UAJ4KOEYMK3VCMPUYZBZBUL7C.PJHNDGTCF26WPTLSNKXNBZDBGCHILX3YNHDAUXY", c.targetUrn );
		assertEquals( 3, c.parentCommitUrns.length );
		assertEquals( "x-rdf-subject:urn:bitprint:35OQJVDODQ4ZRCVD4YS2ZIFLTHHZGT22.QYHXX6XLB6JYAX7P46UKH2VHMOWCXWB4LJV2UGY", c.parentCommitUrns[0] );
		assertEquals( "x-rdf-subject:urn:bitprint:75OQJVDODQ4ZRCVD4YS2ZIFLTHHZGT22.QYHXX6XLB6JYAX7P46UKH2VHMOWCXWB4LJV2UGY", c.parentCommitUrns[1] );
		assertEquals( "x-rdf-subject:urn:bitprint:85OQJVDODQ4ZRCVD4YS2ZIFLTHHZGT22.QYHXX6XLB6JYAX7P46UKH2VHMOWCXWB4LJV2UGY", c.parentCommitUrns[2] );
	}
}
