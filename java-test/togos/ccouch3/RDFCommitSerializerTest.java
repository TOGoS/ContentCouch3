package togos.ccouch3;

import java.io.ByteArrayOutputStream;
import java.text.ParseException;

import junit.framework.TestCase;
import togos.ccouch3.util.DateUtil;

public class RDFCommitSerializerTest extends TestCase
{
	protected static final String SOME_COMMIT_XML =
		"<Commit xmlns=\"http://ns.nuke24.net/ContentCouch/\" xmlns:dc=\"http://purl.org/dc/terms/\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
		"\t<parent rdf:resource=\"x-parse-rdf:urn:sha1:C7UVY35UACWFGMH2JUGBGV7ETZBKCBVW\"/>\n" +
		"\t<target rdf:resource=\"x-parse-rdf:urn:sha1:6MDCMMQIY3K6PSNHR3MLCFIQL2PUKTDJ\"/>\n" +
		"\t<dc:created>2010-09-09 04:04:02 GMT</dc:created>\n" +
		"\t<dc:creator>togos-win-backup</dc:creator>\n" +
		"\t<dc:description>TOGoS image archives on togos-win, 2010-09-08</dc:description>\n" +
		"</Commit>\n";
	
	static final long SOME_COMMIT_TIMESTAMP;
	static {
		long ts = 0;
		try {
			ts = DateUtil.parseDate("2010-09-09 04:04:02 GMT").getTime();
		} catch( ParseException e ) {
			throw new RuntimeException(e);
		} finally {
			SOME_COMMIT_TIMESTAMP = ts;
		}
	}
	
	protected static final Commit SOME_COMMIT = new Commit(
		"x-parse-rdf:urn:sha1:6MDCMMQIY3K6PSNHR3MLCFIQL2PUKTDJ",
		new String[] { "x-parse-rdf:urn:sha1:C7UVY35UACWFGMH2JUGBGV7ETZBKCBVW" },
		new String[] {},
		"togos-win-backup",
		"TOGoS image archives on togos-win, 2010-09-08",
		SOME_COMMIT_TIMESTAMP
	);
	
	
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
		
	static final long SOME_COMMIT_2_TIMESTAMP;
	
	static {
		long ts = 0;
		try {
			ts = DateUtil.parseDate("2012-05-10 05:28:42 GMT").getTime();
		} catch( ParseException e ) {
			throw new RuntimeException(e);
		} finally {
			SOME_COMMIT_2_TIMESTAMP = ts;
		}
	}
	
	protected static final Commit SOME_COMMIT_2 = new Commit(
		"x-rdf-subject:urn:bitprint:QWKZDK5UAJ4KOEYMK3VCMPUYZBZBUL7C.PJHNDGTCF26WPTLSNKXNBZDBGCHILX3YNHDAUXY",
		new String[] {
			"x-rdf-subject:urn:bitprint:75OQJVDODQ4ZRCVD4YS2ZIFLTHHZGT22.QYHXX6XLB6JYAX7P46UKH2VHMOWCXWB4LJV2UGY",
			"x-rdf-subject:urn:bitprint:85OQJVDODQ4ZRCVD4YS2ZIFLTHHZGT22.QYHXX6XLB6JYAX7P46UKH2VHMOWCXWB4LJV2UGY",
			"x-rdf-subject:urn:bitprint:35OQJVDODQ4ZRCVD4YS2ZIFLTHHZGT22.QYHXX6XLB6JYAX7P46UKH2VHMOWCXWB4LJV2UGY"
		},
		new String[] { "foo", "bar", "baz" },
		"maude-trtd",
		"TOGoS image archive on Maude, 2012-05-10 00:28:33",
		SOME_COMMIT_2_TIMESTAMP
	);
	
	
	protected void setUp() {
		assertEquals( 1284005042000l, SOME_COMMIT_TIMESTAMP );
	}
	
	protected void assertCommitSerializesTo( String expected, Commit c ) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		new RDFCommitSerializer().serializeCommit( c, baos );
		String serialized = baos.toString("UTF-8");
		assertEquals( expected, serialized );
	}
	
	public void testSerializeSomeCommit() throws Exception {
		assertCommitSerializesTo( SOME_COMMIT_XML, SOME_COMMIT );
	}
	
	public void testSerializeSomeCommit2() throws Exception {
		assertCommitSerializesTo( SOME_COMMIT_2_XML, SOME_COMMIT_2 );
	}
}
