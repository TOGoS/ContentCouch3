package togos.ccouch3;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import togos.blob.ByteBlob;
import togos.blob.file.FileBlob;
import togos.blob.util.BlobUtil;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;
import togos.ccouch3.xml.XML;
import togos.tinywebserver.WebServer;
import togos.tinywebserver.WebServer.HTTPRequest;
import togos.tinywebserver.WebServer.HTTPRequestHandler;
import togos.tinywebserver.WebServer.HTTPResponse;

public class WebServerCommand
{
	static class Mount {
		public final String webPath;
		public final File fsPath;
		public final boolean haltsSearch;
		
		public Mount( String webPath, File fsPath, boolean haltsSearch ) {
			this.webPath = webPath;
			this.fsPath = fsPath;
			this.haltsSearch = haltsSearch;
		}
	}
	
	static final int DEFAULT_PORT = 14567;
	
	public final ArrayList<Repository> repositories = new ArrayList<Repository>();
	public final ArrayList<Mount> mounts = new ArrayList<Mount>();
	public int port = DEFAULT_PORT;
	Map<String,String> filenameExtensionMimeTypes = new HashMap<String,String>();
	String defaultMimeType = "text/plain";
	
	public WebServerCommand() {
		filenameExtensionMimeTypes.put("png", "image/png");
		filenameExtensionMimeTypes.put("jpg", "image/jpeg");
		filenameExtensionMimeTypes.put("jpeg", "image/jpeg");
		filenameExtensionMimeTypes.put("svg", "image/svg");
		filenameExtensionMimeTypes.put("html", "text/html");
		filenameExtensionMimeTypes.put("txt", "text/plain");
		filenameExtensionMimeTypes.put("json", "application/json");
		// TODO: Allow loading from mime.types-like file
		// Probably add MIME type guessing (based on filename and/or magic) to togos.tinywebserver
	}
	
	protected static String urlEncodeString(String text) {
		try {
			return URLEncoder.encode(text, "UTF-8");
		} catch( UnsupportedEncodingException e ) {
			throw new RuntimeException(e);
		}
	}
	
	protected static String urlDecodeString(String enc) {
		try {
			return URLDecoder.decode(enc, "UTF-8");
		} catch( UnsupportedEncodingException e ) {
			throw new RuntimeException(e);
		}
	}
	
	final Pattern filenameExtensionPattern = Pattern.compile("^.*\\.([^\\./]+)$");
	String guessContentTypeFromFilename(String filename) {
		Matcher m = filenameExtensionPattern.matcher(filename);
		if (!m.matches()) return null;
		String ext = m.group(1).toLowerCase();
		String mimeType = filenameExtensionMimeTypes.get(ext);
		return mimeType;
	}
	
	class RequestHandler implements HTTPRequestHandler
	{
		final Pattern rawPattern = Pattern.compile("^/uri-res/raw/([^/]+)(?:/([^/]+))?$"); 
		final Pattern n2rPattern = Pattern.compile("^/uri-res/N2R\\?(.*)$");
		
		protected HTTPResponse handleUriRes(HTTPRequest req) {
			Matcher m;
			String urn;
			String filenameHint = null;
			if( (m = rawPattern.matcher(req.path)).matches() ) {
				urn = urlDecodeString(m.group(1));
				filenameHint = m.group(2);
			} else if( (m = n2rPattern.matcher(req.path)).matches() ) {
				urn = urlDecodeString(m.group(1));
			} else {
				return null;
			}
			
			String contentType = null;
			if (filenameHint != null) contentType = guessContentTypeFromFilename(filenameHint);
			if (contentType == null) contentType = defaultMimeType;
			if (contentType == null) contentType = "application/octet-stream"; // Our code below requires *something*
			
			for( Repository r : repositories ) {
				ByteBlob b;
				try {
					b = r.getBlob(urn);
				} catch( IOException e ) {
					continue;
				}
				if( b != null ) {
					return new HTTPResponse("HTTP/1.0", 200, "Okay",
						WebServer.mkHeaders("content-type",contentType),
						b
					);
				}
			}
			
			return null;
		}

		protected HTTPResponse mk404(HTTPRequest req) {
			return new HTTPResponse("HTTP/1.0", 404, "Not Found",
				togos.tinywebserver.WebServer.mkHeaders("content-type","text/plain"),
				BlobUtil.byteChunk(req.path+" doesn't exist on this server")
			);
		}
		
		protected HTTPResponse fileResponse(File f) {
			return new HTTPResponse("HTTP/1.0", 200, "Okay",
				WebServer.mkHeaders("content-type", guessContentTypeFromFilename(f.getName()), "content-length", String.valueOf(f.length())),
				new FileBlob(f)
			);
		}
		
		protected HTTPResponse handleMounted(HTTPRequest req) {
			String reqPath = urlDecodeString(req.path.split("\\?",1)[0]);
			ArrayList<File> matchingFiles = new ArrayList<File>();
			boolean matchingMountsFound = false;
			for( Mount mount : mounts ) {
				String subPath;
				if( reqPath.equals(mount.webPath) ) {
					subPath = "";
				} else if( reqPath.startsWith(mount.webPath+"/") ) {
					subPath = reqPath.substring(mount.webPath.length()+1);
				} else if( mount.webPath.endsWith("/") && reqPath.startsWith(mount.webPath) ) {
					subPath = reqPath.substring(mount.webPath.length());
				} else {
					subPath = null;
				}
				System.err.println(mount.fsPath+" ; "+subPath);
				if( subPath != null ) {
					matchingMountsFound = true;
					File f = "".equals(subPath) ? mount.fsPath : new File(mount.fsPath, subPath);
					if( f.exists() ) matchingFiles.add(f);
					if( mount.haltsSearch ) break;
				}
			}
			
			if( !matchingMountsFound ) return null;
			
			if( matchingFiles.size() == 0 ) return mk404(req);
			
			for( File f : matchingFiles ) {
				if( !f.isDirectory() ) return fileResponse(f);
			}
			
			HashMap<String,File> subs = new HashMap<String,File>();
			// Otherwise they're all directories.  Merge their listings together!
			for( File d : matchingFiles ) {
				for( File f : d.listFiles() ) {
					subs.put(f.getName(), f);
				}
			}
			
			ArrayList<Map.Entry<String,File>> entries = new ArrayList<Map.Entry<String,File>>(subs.entrySet());
			Collections.sort(entries, new Comparator<Map.Entry<String,File>>() {
				@Override public int compare(Entry<String, File> e1, Entry<String, File> e2) {
					boolean isDir1 = e1.getValue().isDirectory();
					boolean isDir2 = e2.getValue().isDirectory();
					if( isDir1 && !isDir2 ) return -1;
					if( isDir2 && !isDir1 ) return +1;
					return e1.getKey().compareTo(e2.getKey());
				}
			});
			
			StringBuilder rows = new StringBuilder();
			for( Map.Entry<String,File> entry : entries ) {
				boolean isDir = entry.getValue().isDirectory();
				String name = entry.getKey() + (isDir?"/":"");
				String href;
				try {
					href = URLEncoder.encode(entry.getKey(), "UTF-8") + (isDir?"/":"");
				} catch( UnsupportedEncodingException e ) {
					throw new RuntimeException(e);
				}
				String sizeText = isDir ? "" : String.valueOf(entry.getValue().length());
				String lastModText = new Date(entry.getValue().lastModified()).toString();
				rows.append("<tr>"+
					"<td><a href=\""+XML.xmlEscapeAttributeValue(href)+"\">"+XML.xmlEscapeText(name)+"</a></td>"+
					"<td>"+XML.xmlEscapeText(sizeText)+"</td>"+
					"<td>"+XML.xmlEscapeText(lastModText)+"</td>"+
					"</tr>\n");
			}
			
			String listing =
				"<html><head><title>"+XML.xmlEscapeText("Index of "+req.path)+"</title></head><body>\n"+
				"<table>\n"+
				rows +
				"</table>\n"+
				"</body></html>\n";
			
			return new HTTPResponse(
				"HTTP/1.0", 200, "Okay",
				WebServer.mkHeaders("content-type", "text/html"),
				BlobUtil.byteChunk(listing)
			);
					
		}
		
		@Override public HTTPResponse handle(HTTPRequest req) {
			HTTPResponse res = handleMounted(req);
			if( res == null ) res = handleUriRes(req);
			return res == null ? mk404(req) : res;
		}
	}
	
	public void addMount( String webPath, String fsPath, boolean haltsSearch ) {
		if( !webPath.startsWith("/") ) webPath = "/"+webPath;
		
		File f = new File(fsPath);
		mounts.add(new Mount(webPath, f, haltsSearch));
	}
	
	public void run() {
		RequestHandler rh = new RequestHandler();
		WebServer ws = new WebServer(rh);
		ws.port = port;
		ws.run();
	}
	
	protected static final String USAGE_TEXT =
		"Usage: ccouch3 web-server [options]\n"+
		"Options:\n"+
		String.format("  -port %5d                     ; Indicate port to listen on\n", DEFAULT_PORT)+
		"  -repo[:name] /path/to/repo      ; Add a repository to back N2R requests\n"+
		"  -file[:/webpath] /path/to/file  ; Serve a file or directory at /webpath\n"+
		"  -union[:/webpath] /path/to/file ; Same, but allow falling through to the\n"+
		"                                  ; next mount or union, and merging directory\n"+
		"                                  ; listings with them.";
	
	public static int main(Iterator<String> argi) {
		WebServerCommand wsc = new WebServerCommand();
		while( argi.hasNext() ) {
			String arg = argi.next();
			if( "-port".equals(arg) ) {
				wsc.port = Integer.parseInt(argi.next());
			} else if( "-repo".equals(arg) || arg.startsWith("-repo:") ) {
				wsc.repositories.add(new SHA1FileRepository(new File(argi.next(), "data"), "bro"));
			} else if( "-union".equals(arg) ) {
				String fsPath = argi.next();
				wsc.addMount("/", fsPath, false);
			} else if( arg.startsWith("-union:") ) {
				String fsPath = argi.next();
				wsc.addMount(arg.substring(7), fsPath, false);
			} else if( "-file".equals(arg) ) {
				String fsPath = argi.next();
				wsc.addMount("/", fsPath, true);
			} else if( arg.startsWith("-file:") ) {
				String fsPath = argi.next();
				wsc.addMount(arg.substring(7), fsPath, true);
			} else if( CCouch3Command.isHelpArgument(arg) ) {
				System.out.println(USAGE_TEXT);
				return 0;
			} else {
				System.err.println("Unrecognized option: "+arg);
				System.err.println(USAGE_TEXT);
				return 1;
			}
		}
		wsc.run();
		return 1;
	}
	
	public static void main(String[] args) {
		System.exit( main( Arrays.asList(args).iterator()) );
	}
}
