package togos.ccouch3;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import togos.ccouch3.util.DateUtil;
import togos.ccouch3.util.XMLUtil;

public class RDFCommitSerializer
{
	public void serializeCommit( Commit m, OutputStream os ) throws IOException {
		boolean needDcNamespace = m.authorName != null || m.description != null || m.timestamp != Commit.TIMESTAMP_NONE;
		
		OutputStreamWriter w = new OutputStreamWriter(os);
		
		List<String> tags = Arrays.asList(m.tags);
		Collections.sort(tags);

		List<String> parentUrns = Arrays.asList(m.parentCommitUrns);
		Collections.sort(parentUrns);
		
		w.write("<Commit");
		w.write(" xmlns=\"http://ns.nuke24.net/ContentCouch/\"");
		if( needDcNamespace ) w.write(" xmlns:dc=\"http://purl.org/dc/terms/\"");
		w.write(" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"");
		w.write(">\n");
		for( String parentUrn : parentUrns ) {
			w.write("\t<parent rdf:resource=\"");
			w.write(XMLUtil.xmlEscapeAttribute(parentUrn));
			w.write("\"/>\n");
		}
		for( String tag : tags ) {
			w.write("\t<tag>");
			w.write(XMLUtil.xmlEscapeText(tag));
			w.write("</tag>\n");
		}
		w.write("\t<target rdf:resource=\"");
		w.write(XMLUtil.xmlEscapeAttribute(m.targetUrn));
		w.write("\"/>\n");
		if( m.timestamp != Commit.TIMESTAMP_NONE ) {
			w.write("\t<dc:created>");
			w.write(XMLUtil.xmlEscapeText(DateUtil.formatDate(m.timestamp)));
			w.write("</dc:created>\n");
		}
		if( m.authorName != null ) {
			w.write("\t<dc:creator>");
			w.write(XMLUtil.xmlEscapeText(m.authorName));
			w.write("</dc:creator>\n");
		}
		if( m.description != null ) {
			w.write("\t<dc:description>");
			w.write(XMLUtil.xmlEscapeText(m.description));
			w.write("</dc:description>\n");
		}
		w.write("</Commit>\n");
		
		w.flush();
	}
}
