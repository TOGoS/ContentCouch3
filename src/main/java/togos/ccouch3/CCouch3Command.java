package togos.ccouch3;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;

public class CCouch3Command
{
	public static class GeneralOptions {
		static final int VERBOSITY_SILENT = 0;
		static final int VERBOSITY_ERRORS = 100;
		static final int VERBOSITY_WARNINGS = 200;
		static final int VERBOSITY_USAGE_WARNINGS = 300;
		static final int VERBOSITY_DEFAULT = VERBOSITY_USAGE_WARNINGS;
		static final int VERBOSITY_VERBOSE = 400;
		static final int VERBOSITY_VERY_VERBOSE = 500;
		
		RepoConfig repoConfig = new RepoConfig();
		int verbosityLevel;
		
		public boolean parseCommandLineArg( String arg, Iterator<String> argi ) {
			if( repoConfig.parseCommandLineArg(arg, argi) ) return true;
			
			if( "-q".equals(arg) ) { verbosityLevel = VERBOSITY_ERRORS; return true; }
			if( "-v".equals(arg) ) { verbosityLevel = VERBOSITY_VERBOSE; return true; }
			if( "-vv".equals(arg) ) { verbosityLevel = VERBOSITY_VERY_VERBOSE; return true; }
			
			return false;
		}
	}
	
	public static LiberalFileResolver getCommandLineFileResolver(Repository[] repos, File[] repoDirs) {
		File[] headRoots = new File[repoDirs.length];
		for( int i=0; i<repoDirs.length; ++i ) {
			headRoots[i] = new File(repoDirs[i], "heads");
		}
		return new LiberalFileResolver(repos, new CCouchHeadResolver(headRoots));
	}
	
	public static LiberalFileResolver getCommandLineFileResolver(File[] repoDirs) {
		Repository[] repos = new Repository[repoDirs.length];
		for( int i=0; i<repoDirs.length; ++i ) {
			repos[i] = new SHA1FileRepository(new File(repoDirs[i],"data"),null);
		}
		return getCommandLineFileResolver(repos, repoDirs);
	}

	public static LiberalFileResolver getCommandLineFileResolver( RepoConfig config ) {
		return getCommandLineFileResolver(config.getRepoDirs());
	}

	////
	
	public static boolean isHelpArgument( String arg ) {
		return
			"-?".equals(arg) || "-h".equals(arg) ||
			"--help".equals(arg) || "-help".equals(arg) || "-halp".equals(arg);
	}
	
	public static String USAGE =
		"Usage: ccouch3 <subcommand> <subcommand-arguments>\n" +
		"Subcommands:\n" +
		"  identify          ; identify files/directories\n" +
		"  upload            ; upload files to a remote repository\n" +
		"  cache             ; download and cache files from remote repositories\n" +
		"  cat               ; output blobs to standard output\n" +
		"  config            ; dump configuration to stdout\n" +
		"  copy              ; copy files\n" +
		"  backup            ; back up files locally (simplified implementation)\n" +
		"  command-server    ; run a command server\n" +
		"  web-server        ; run a web server\n" +
		"  store-stream      ; store files or pipe contents\n"+
		"  annotate-m3u      ; add #URN lines to an M3U file\n"+
		"  <subcommand> -?   ; get help on a specific command\n"+
		"\n"+
		"'backup' exists primarily because its implementation is simpler\n"+
		"and easier to debug than that of 'upload';\n"+
		"Otherwise they serve similar purposes.\n";
	
	public static int main( Iterator<String> argi ) throws Exception {
		GeneralOptions gOpts = new GeneralOptions();
		while( argi.hasNext() ) {
			String cmd = argi.next();
			// TODO: Update commands to take general options
			if( gOpts.parseCommandLineArg(cmd, argi) ) {
			} else if( "upload".equals(cmd) ) {
				return FlowUploader.uploadMain(argi);
			} else if( "cache".equals(cmd) ) {
				return Downloader.main(argi);
			} else if( "cat".equals(cmd) ) {
				return Cat.main(argi);
			} else if( "copy".equals(cmd) ) {
				return Copy.main(gOpts, argi);
			} else if( "config".equals(cmd) ) {
				return ConfigDump.main(gOpts, argi);
			} else if( "backup".equals(cmd) ) {
				return UpBacker.backupMain(gOpts, argi);
			} else if( "store-stream".equals(cmd) ) {
				return StoreStream.main(gOpts, argi);
			} else if( "id".equals(cmd) || "identify".equals(cmd) ) {
				return FlowUploader.identifyMain(argi);
			} else if( "cmd-server".equals(cmd) || "command-server".equals(cmd) ) {
				return CmdServer.main(argi);
			} else if( "web-server".equals(cmd) || "ws".equals(cmd) || "webserv".equals(cmd) ) {
				return WebServerCommand.main(argi);
			} else if( "verify-tree".equals(cmd) ) {
				return TreeVerifier.main(argi);
			} else if( "extract-urns".equals(cmd) ) {
				return BlobReferenceScanner.main(argi);
			} else if( "annotate-m3u".equals(cmd) ) {
				return M3UAnnotator.main(argi);
			} else if( "help".equals(cmd) || isHelpArgument(cmd) ) {
				System.out.print(USAGE);
				return 0;
			} else {
				System.err.println("Error: Unrecognized command: "+cmd);
				System.err.print(USAGE);
				return 1;
			}
		}
		
		System.err.println("Error: no subcommand given.");
		System.err.print(USAGE);
		return 1;
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit( main( Arrays.asList(args).iterator()) );
	}
}
