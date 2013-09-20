package togos.ccouch3.download;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import togos.ccouch3.ActiveJobSet;

public class Downloader
{
	final ActiveJobSet<String> activeJobSet = new ActiveJobSet<String>();
	final ArrayBlockingQueue<CacheJob> cacheJobQueue = new ArrayBlockingQueue<CacheJob>(20);
	
	final CacheJobSubmitter cacheJobSubmitter;
	final CacheJobProcessor cacheJobProcessor;
	final DownloadJobProcessor downloadJobProcessor;
	final DownloadJobCompleter downloadJobCompleter;
	
	static class InputFormatException extends Exception {
		private static final long serialVersionUID = -4926065205980857344L;

		public InputFormatException( String msg ) {
			super(msg);
		}
	}
	
	CacheJobSubmitter cjc = new CacheJobSubmitter();
	
	protected void startDownload( String arg, BufferedReader stdin, String filename )
		throws InputFormatException, IOException, InterruptedException
	{
		if( arg.startsWith("@") && arg.length() > 1 ) {
			String fn = arg.substring(1);
			if( "-".equals(fn) ) {
				if( stdin == null ) {
					throw new InputFormatException("'@-' does not make sense in this context ("+filename+")");
				}
				startDownloads( stdin, "standard input" );
				stdin.close(); // Don't let nobody use '-' twice; the behavior would be undefined!
			} else {
				FileReader fr = new FileReader(fn);
				try {
					startDownloads( new BufferedReader(fr), fn );
				} finally {
					fr.close();
				}
			}
		} else {
			cjc.addUrn(arg);
		}
	}
	
	protected void startDownloads( BufferedReader urnStream, String filename )
		throws IOException, InputFormatException, InterruptedException
	{
		String line;
		while( (line = urnStream.readLine()) != null ) {
			line = line.trim();
			if( line.isEmpty() || line.startsWith("#") ) continue;
			startDownload( line, null, filename );
		}
	}
	
	public void download( List<String> urns, BufferedReader stdin, String context )
		throws IOException, InputFormatException, InterruptedException
	{
		ActiveJobSet<Object> taskCounter = new ActiveJobSet<Object>();
		for( String urn : urns ) startDownload(urn, stdin, context);
		taskCounter.waitUntilEmpty();
	}
	
	public static int downloadMain( Iterator<String> args )
		throws IOException, InterruptedException
	{
		List<String> urns = new ArrayList<String>();
		
		while( args.hasNext() ) {
			String arg = args.next();
			if( !arg.startsWith("-") ) {
				urns.add(arg);
			} else {
				System.err.println("Error: unrecognized option: '"+arg+"'");
				return 1;
			}
		}
		
		try {
			new Downloader().download(urns, new BufferedReader(new InputStreamReader(System.in)), "command-line arguments");
		} catch( InputFormatException e ) {
			System.err.println(e.getMessage());
			return 1;
		}
		
		return 0;
	}
}
