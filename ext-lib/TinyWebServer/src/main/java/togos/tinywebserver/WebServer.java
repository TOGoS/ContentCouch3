package togos.tinywebserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import togos.blob.ByteBlob;
import togos.blob.util.BlobUtil;
import togos.blob.util.SimpleByteChunk;
import togos.service.Service;

public class WebServer implements Runnable, Service {
	protected static String[] add(String[] list, String item) {
		if( list == null ) list = new String[1]; 
		else list = Arrays.copyOf(list, list.length+1);
		list[list.length-1] = item;
		return list;
	}
	
	public static Map<String,String[]> mkHeaders(String...kv) {
		LinkedHashMap<String,String[]> headers = new LinkedHashMap<String,String[]>(); 
		for( int i=0; i+1<kv.length; i += 2 ) {
			headers.put(kv[i], add(headers.get(kv[i]), kv[i+1]));
		}
		return headers;
	}
	
	public static class HTTPRequest {
		public final String method;
		public final String path;
		public final String protocol;
		public final Map<String,String[]> headers;
		public final InputStream contentStream;
		
		public HTTPRequest( String method, String path, String protocol, Map<String,String[]> headers, InputStream contentStream ) {
			this.method = method;
			this.path = path;
			this.protocol = protocol;
			this.headers = headers;
			this.contentStream = contentStream;
		}
	}
	
	public static class HTTPResponse {
		public final String protocol;
		public final int statusCode;
		public final String statusText;
		public final Map<String,String[]> headers;
		public final ByteBlob content;
		
		public HTTPResponse(String protocol, int statusCode, String statusText, Map<String,String[]> headers, ByteBlob content) {
			this.protocol = protocol;
			this.statusCode = statusCode;
			this.statusText = statusText;
			this.headers = headers;
			this.content = content;
		}
	}
	
	public interface HTTPRequestHandler {
		HTTPResponse handle(HTTPRequest req);
	}
	
	class HTTPConnectionHandler implements Runnable {
		protected Socket cs;
		
		public HTTPConnectionHandler( Socket cs ) {
			this.cs = cs;
		}
		
		protected String getConnectionErrorDescription() {
			SocketAddress remoteAddr = cs.getRemoteSocketAddress();
			if( remoteAddr != null ) {
				return "Error handling connection from "+remoteAddr;
			} else {
				return "Error handling connection";
			}
		}
		
		// TODO: something faster
		// Can't use BufferedReader because it reads too much
		// and then later content = read(...) misses data.
		protected String readLine(InputStream is) throws IOException {
			int byt;
			String line = "";
			while( (byt = is.read()) != -1 && byt != '\n' ) {
				if( byt != '\r' ) line += (char)byt;
			}
			return line;
		}
		
		public void run() {
			try {
				InputStream is = cs.getInputStream();
				String rl = readLine(is);
				if( rl == null ) return;
				String hl = readLine(is);
				HashMap<String,String[]> headers = new HashMap<String,String[]>();
				while( hl != null && hl.length() > 0 ) {
					String[] kv = hl.trim().split(": ");
					String key = kv[0].toLowerCase();
					headers.put(key, add(headers.get(key), kv[1]));
					hl = readLine(is);
				}
				
				String[] rp = rl.split("\\s", 3);
				HTTPResponse res;
				if( rp.length < 3 ) {
					res = new HTTPResponse("HTTP/1.0", 400, "Malformed request",
						mkHeaders("content-type", "text/plain"),
						BlobUtil.byteChunk("Your request line,\n  "+rl+"\ndoesn't seem to have enough parts")
					);
				} else {
					HTTPRequest req = new HTTPRequest(rp[0], rp[1], rp[2], headers, is);
					res = requestHandler.handle(req);
				}
				
				OutputStream os = cs.getOutputStream();
				String responseHeaders = res.protocol+" "+res.statusCode+" "+res.statusText+"\r\n";
				if( res.content != null && !res.headers.containsKey("content-length")) {
					long len = res.content.getSize();
					if( len >= 0 ) {
						res.headers.put("content-length", new String[]{ Long.toString(len) });
					}
				}
				for( Map.Entry<String,String[]> header : res.headers.entrySet() ) {
					for( String headerValue : header.getValue() ) {
						responseHeaders += header.getKey()+": "+headerValue+"\r\n";
					}
				}
				responseHeaders += "\r\n";
				os.write(responseHeaders.getBytes("ASCII"));
				if( res.content != null ) {
					res.content.writeTo(os);
				}
				os.flush();
			} catch( UnsupportedEncodingException e ) {
				System.err.println(getConnectionErrorDescription());
				e.printStackTrace();
			} catch( IOException e ) {
				System.err.println(getConnectionErrorDescription());
				e.printStackTrace();
			} finally {
				try {
					if( !cs.isClosed() ) cs.close();
				} catch( IOException e ) {}
			}
		}
	}
	
	public int port = 14419;
	public final HTTPRequestHandler requestHandler;
	
	public void run() {
		try {
			ServerSocket ss = new ServerSocket(port);
			try {
				while( true ) {
					Socket clientSock = ss.accept();
					new Thread(new HTTPConnectionHandler(clientSock)).start();
				}
			} finally {
				ss.close();
			}
		} catch( IOException e ) {
			throw new RuntimeException(e);
		}
	}
	
	Thread runThread; 
	
	@Override public synchronized void start() {
		if( runThread != null ) return;
		runThread = new Thread(this);
		runThread.start();
	}
	@Override public synchronized void halt() {
		if( runThread == null ) return;
		runThread.interrupt();
		runThread = null;
	}
	@Override public void join() throws InterruptedException {
		Thread runThread = this.runThread;
		if( runThread == null ) return;
		runThread.join();
	}
	
	public WebServer(HTTPRequestHandler rh) {
		this.requestHandler = rh;
	}
	
	public static final String USAGE =
		"Usage: WebServer [-port <listen-port>]";
	
	public static void main(String[] args) {
		WebServer ws = new WebServer(new HTTPRequestHandler() {
			@Override public HTTPResponse handle(HTTPRequest req) {
				return new HTTPResponse("HTTP/1.0", 200, "Hi",
					mkHeaders("content-type","text/plain"),
					new SimpleByteChunk("Hello, world!".getBytes()));
			}
		});
		
		for( int i=0; i<args.length; ++i ) {
			if( "-port".equals(args[i]) ) {
			    ws.port = Integer.parseInt(args[++i]);
			} else {
				System.err.println("Unrecognised argument: "+args[i]);
				System.err.println(USAGE);
				System.exit(1);
			}
		}
		
		try {
			ws.start();
			System.err.println("Listening on port "+ws.port);
		} catch( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
