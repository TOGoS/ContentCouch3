package togos.ccouch3;

import java.util.Arrays;
import java.util.Iterator;

public class CCouch3Command
{
	public static int main( Iterator<String> argi ) throws Exception {
		if( !argi.hasNext() ) {
			System.err.println("Error: no command given.");
			System.err.println("Usage: ccouch3 <command> [options]");
			return 1;
		}
		String cmd = argi.next();
		if( "upload".equals(cmd) ) {
			return FlowUploader.uploadMain(argi);
		} else if( "id".equals(cmd) ) {
			return FlowUploader.identifyMain(argi);
		} else if( "cmd-server".equals(cmd) ) {
			return CmdServer.main(argi);
		} else {
			System.err.println("Error: Unrecognised command: "+cmd);
			return 1;
		}
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit( main( Arrays.asList(args).iterator()) );
	}
}
