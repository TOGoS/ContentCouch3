package togos.ccouch3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;

import junit.framework.TestCase;
import togos.ccouch3.util.DateUtil;

public class NewStyleRDFDirectorySerializerTest extends TestCase
{
	NewStyleRDFDirectorySerializer serializer = new NewStyleRDFDirectorySerializer();
	
	protected void assertSerializesTo( String expected, Collection<DirectoryEntry> entries ) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			serializer.serialize( entries, baos );
			assertEquals( expected, baos.toString("UTF-8") );
		} catch( UnsupportedEncodingException e ) {
			throw new RuntimeException(e);
		} catch( IOException e ) {
			throw new RuntimeException(e);
		}
	}
	
	protected void assertSerializesTo( String expected, DirectoryEntry[] entries ) {
		assertSerializesTo( expected, Arrays.asList(entries) );
	}
	
	protected static final String blobEntryString =
		"<Directory xmlns=\"http://ns.nuke24.net/ContentCouch/\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
		"\t<entries rdf:parseType=\"Collection\">\n" +
		"\t\t<DirectoryEntry>\n" +
		"\t\t\t<name>foo</name>\n" +
		"\t\t\t<target>\n" +
		"\t\t\t\t<Blob rdf:about=\"urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZO\"/>\n" +
		"\t\t\t</target>\n" +
		"\t\t</DirectoryEntry>\n" +
		"\t</entries>\n" +
		"</Directory>\n";
	
	public void testBlobEntry() {
		assertSerializesTo( blobEntryString, new DirectoryEntry[] {
			new DirectoryEntry("foo", "urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZO", FSObjectType.BLOB, -1, -1)
		} );
	}
	
	protected static final String multiBlobEntryString =
			"<Directory xmlns=\"http://ns.nuke24.net/ContentCouch/\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
			"\t<entries rdf:parseType=\"Collection\">\n" +
			"\t\t<DirectoryEntry>\n" +
			"\t\t\t<name>boo</name>\n" +
			"\t\t\t<target>\n" +
			"\t\t\t\t<Blob rdf:about=\"urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZB\"/>\n" +
			"\t\t\t</target>\n" +
			"\t\t</DirectoryEntry>\n" +
			"\t\t<DirectoryEntry>\n" +
			"\t\t\t<name>doo</name>\n" +
			"\t\t\t<target>\n" +
			"\t\t\t\t<Blob rdf:about=\"urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZD\"/>\n" +
			"\t\t\t</target>\n" +
			"\t\t</DirectoryEntry>\n" +
			"\t\t<DirectoryEntry>\n" +
			"\t\t\t<name>foo</name>\n" +
			"\t\t\t<target>\n" +
			"\t\t\t\t<Blob rdf:about=\"urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZF\"/>\n" +
			"\t\t\t</target>\n" +
			"\t\t</DirectoryEntry>\n" +
			"\t\t<DirectoryEntry>\n" +
			"\t\t\t<name>goo</name>\n" +
			"\t\t\t<target>\n" +
			"\t\t\t\t<Blob rdf:about=\"urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZG\"/>\n" +
			"\t\t\t</target>\n" +
			"\t\t</DirectoryEntry>\n" +
			"\t</entries>\n" +
			"</Directory>\n";
		
	public void testMultiBlobEntry() {
		assertSerializesTo( multiBlobEntryString, new DirectoryEntry[] {
			new DirectoryEntry("foo", "urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZF", FSObjectType.BLOB, -1, -1),
			new DirectoryEntry("goo", "urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZG", FSObjectType.BLOB, -1, -1),
			new DirectoryEntry("boo", "urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZB", FSObjectType.BLOB, -1, -1),
			new DirectoryEntry("doo", "urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZD", FSObjectType.BLOB, -1, -1),
		} );
	}
		
	protected static final String blobWithSizeEntryString =
		"<Directory xmlns=\"http://ns.nuke24.net/ContentCouch/\" xmlns:bz=\"http://bitzi.com/xmlns/2002/01/bz-core#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
		"\t<entries rdf:parseType=\"Collection\">\n" +
		"\t\t<DirectoryEntry>\n" +
		"\t\t\t<name>foo</name>\n" +
		"\t\t\t<target>\n" +
		"\t\t\t\t<Blob rdf:about=\"urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZO\">\n" +
		"\t\t\t\t\t<bz:fileLength>1234</bz:fileLength>\n" +
		"\t\t\t\t</Blob>\n" +
		"\t\t\t</target>\n" +
		"\t\t</DirectoryEntry>\n" +
		"\t</entries>\n" +
		"</Directory>\n";
	
	public void testBlobWithSizeEntry() {
		assertSerializesTo( blobWithSizeEntryString, new DirectoryEntry[] {
			new DirectoryEntry("foo", "urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZO", FSObjectType.BLOB, 1234, -1)
		});
	}
	
	protected static final String blobWithSizeAndMtimeEntryString =
		"<Directory xmlns=\"http://ns.nuke24.net/ContentCouch/\" xmlns:bz=\"http://bitzi.com/xmlns/2002/01/bz-core#\" xmlns:dc=\"http://purl.org/dc/terms/\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
		"\t<entries rdf:parseType=\"Collection\">\n" +
		"\t\t<DirectoryEntry>\n" +
		"\t\t\t<name>foo</name>\n" +
		"\t\t\t<target>\n" +
		"\t\t\t\t<Blob rdf:about=\"urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZO\">\n" +
		"\t\t\t\t\t<bz:fileLength>1234</bz:fileLength>\n" +
		"\t\t\t\t</Blob>\n" +
		"\t\t\t</target>\n" +
		"\t\t\t<dc:modified>2010-01-01 06:00:32 GMT</dc:modified>\n" +
		"\t\t</DirectoryEntry>\n" +
		"\t</entries>\n" +
		"</Directory>\n";
	
	public void testBlobWithSizeAndMtimeEntry() throws ParseException {
		assertSerializesTo( blobWithSizeAndMtimeEntryString, new DirectoryEntry[] {
			new DirectoryEntry("foo", "urn:sha1:3CL2HWDOYOLPUXLWTKY6PAP63D7VOYZO", FSObjectType.BLOB, 1234, DateUtil.parseDate("2010-01-01 06:00:32 GMT").getTime())
		});
	}
	
	protected static final String directoryEntryString =
		"<Directory xmlns=\"http://ns.nuke24.net/ContentCouch/\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
		"\t<entries rdf:parseType=\"Collection\">\n" +
		"\t\t<DirectoryEntry>\n" +
		"\t\t\t<name>2010-01</name>\n" +
		"\t\t\t<target>\n" +
		"\t\t\t\t<Directory rdf:about=\"x-rdf-subject:urn:sha1:AO6BBOUDVIXOV6PEDR7GM536K3WLO6ES\"/>\n" +
		"\t\t\t</target>\n" +
		"\t\t</DirectoryEntry>\n" +
		"\t</entries>\n" +
		"</Directory>\n";
	
	public void testDirectoryEntry() throws ParseException {
		assertSerializesTo( directoryEntryString, new DirectoryEntry[] {
			new DirectoryEntry("2010-01", "x-rdf-subject:urn:sha1:AO6BBOUDVIXOV6PEDR7GM536K3WLO6ES", FSObjectType.DIRECTORY, 1234, DateUtil.parseDate("2010-01-01 06:00:32 GMT").getTime())
		});
	}
	
	protected static final String emptyDirectoryString =
		"<Directory xmlns=\"http://ns.nuke24.net/ContentCouch/\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
		"\t<entries rdf:parseType=\"Collection\"/>\n" +
		"</Directory>\n";
	
	public void testEmptyDirectory() throws ParseException {
		assertSerializesTo( emptyDirectoryString, new DirectoryEntry[] {} );
	}
}
