package togos.ccouch3;

import java.util.Arrays;
import java.util.Iterator;


public class CCouch3Command
{
	public static boolean isHelpArgument( String arg ) {
		return "-?".equals(arg) || "-h".equals(arg) ||
				"--help".equals(arg) || "-help".equals(arg) || "-halp".equals(arg);
	}
	
	public static String USAGE =
		"Usage: ccouch3 <subcommand> <subcommand-arguments>\n" +
		"Subcommands:\n" +
		"  identify          ; identify files/directories\n" +
		"  upload            ; upload files to a remote repository\n" +
		"  cache             ; download and cache files from remote repositories\n" +
		"  backup            ; back up files locally\n" +
		"  command-server    ; run a command server\n" +
		"  <subcommand> -?   ; get help on a specific command";
	
	public static int main( Iterator<String> argi ) throws Exception {
		if( !argi.hasNext() ) {
			System.err.println("Error: no subcommand given.");
			System.err.println(USAGE);
			return 1;
		}
		String cmd = argi.next();
		if( "upload".equals(cmd) ) {
			return FlowUploader.uploadMain(argi);
		} else if( "cache".equals(cmd) ) {
			return Downloader.main(argi);
		} else if( "copy".equals(cmd) ) {
			return Copy.main(argi);
		} else if( "backup".equals(cmd) ) {
			return UpBacker.backupMain(argi);
		} else if( "id".equals(cmd) || "identify".equals(cmd) ) {
			return FlowUploader.identifyMain(argi);
		} else if( "cmd-server".equals(cmd) || "command-server".equals(cmd) ) {
			return CmdServer.main(argi);
		} else if( "help".equals(cmd) || isHelpArgument(cmd) ) {
			System.out.println(USAGE);
			return 0;
		} else {
			System.err.println("Error: Unrecognized command: "+cmd);
			System.err.println(USAGE);
			return 1;
		}
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit( main( Arrays.asList(args).iterator()) );
	}
}
