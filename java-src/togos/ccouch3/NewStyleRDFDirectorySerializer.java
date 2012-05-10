package togos.ccouch3;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import togos.ccouch3.util.DateUtil;

public class NewStyleRDFDirectorySerializer implements DirectorySerializer
{
	// TODO: Centralize and make better
	public static String xmlEscapeText( String text ) {
		return text.replaceAll("&","&amp;").replaceAll("<", "&lt;").replaceAll(">","&gt;");
	}
	
	@Override
	public void serialize(Collection<DirectoryEntry> entries, OutputStream os) throws IOException {
		ArrayList<DirectoryEntry> sortedEntries = new ArrayList<DirectoryEntry>( entries );
		Collections.sort( sortedEntries );

		boolean needBzNamespace = false, needDcNamespace = false;
		for( FileInfo f : sortedEntries ) {
			if( f.fileType == FileInfo.FILETYPE_BLOB ) {
				if( f.mtime != -1 ) needDcNamespace = true;
				if( f.size != -1 ) needBzNamespace = true;
			}
		}
		
		Writer w = new OutputStreamWriter( os, "UTF-8" );
		w.write("<Directory xmlns=\"http://ns.nuke24.net/ContentCouch/\"");
		if( needBzNamespace ) w.write(" xmlns:bz=\"http://bitzi.com/xmlns/2002/01/bz-core#\"");
		if( needDcNamespace ) w.write(" xmlns:dc=\"http://purl.org/dc/terms/\"");
		w.write(" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n");
		w.write("\t<entries rdf:parseType=\"Collection\">\n");
		
		for( DirectoryEntry f : sortedEntries ) {
			String tag;
			boolean showSize, showMtime;
			switch( f.fileType ) {
			case( FileInfo.FILETYPE_BLOB ):
				tag = "Blob";
				showSize = f.size != -1;
				showMtime = f.mtime != -1;
				break;
			case( FileInfo.FILETYPE_DIRECTORY ):
				tag = "Directory";
				showSize = false;
				showMtime = false;
				break;
			default: throw new RuntimeException("Don't know how to encode directory entry with file type "+f.fileType);
			}
			
			w.write("\t\t<DirectoryEntry>\n");
			w.write("\t\t\t<name>" + xmlEscapeText(f.name) + "</name>\n");
			w.write("\t\t\t<target>\n");
			w.write("\t\t\t\t<" + tag + " rdf:about=\"" + xmlEscapeText(f.urn) + "\"");
			if( showSize ) {
				w.write(">\n");
				w.write( "\t\t\t\t\t<bz:fileLength>" + String.valueOf(f.size) + "</bz:fileLength>\n" );
				w.write("\t\t\t\t</" + tag + ">\n");
			} else {
				w.write("/>\n");
			}
			w.write("\t\t\t</target>\n");
			if( showMtime ) {
				w.write( "\t\t\t<dc:modified>" + DateUtil.formatDate(f.mtime) + "</dc:modified>\n" );
			}
			w.write("\t\t</DirectoryEntry>\n");
		}
		w.write("\t</entries>\n");
		w.write("</Directory>\n");
		w.flush();
	}
}
