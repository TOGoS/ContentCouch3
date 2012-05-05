package togos.ccouch3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import togos.ccouch3.FlowUploader.UploadTask;

public class CCouch3Command
{
	public int runStore( List<String> args ) throws Exception {
		ArrayList<UploadTask> uploadTasks = new ArrayList<UploadTask>();
		for( Iterator<String> argi=args.iterator(); argi.hasNext(); ) {
			String arg = argi.next();
			if( !arg.startsWith("-") ) {
				uploadTasks.add(new UploadTask(arg,arg));
			} else {
				throw new RuntimeException("Unrecognised argument: " + arg);
			}
		}
		
		new FlowUploader( uploadTasks ).run();
		return 0;
	}
	
	static final <E> List<E> tail( List<E> l ) {
		return l.subList( 1, l.size() );
	}
	
	public int run( List<String> args ) throws Exception {
		if( args.size() == 0 ) {
			System.err.println("Error: no command given.");
			System.err.println("Usage: ccouch3 <command> [options]");
			return 1;
		}
		String cmd = args.get(0);
		if( "store".equals(cmd) ) {
			return runStore(tail(args));
		} else if( "cmd-server".equals(cmd) ) {
			return CmdServer.run(tail(args));
		} else {
			System.err.println("Error: Unrecognised command: "+cmd);
			return 1;
		}
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit( new CCouch3Command().run( Arrays.asList(args) ) );
	}
}
