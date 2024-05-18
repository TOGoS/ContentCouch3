package togos.ccouch3;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Iterator;

import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;
import togos.ccouch3.util.Consumer;
import togos.ccouch3.util.Action;

public class CCouch3Command
{
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

	public static LiberalFileResolver getCommandLineFileResolver(CCouchContext ctx) {
		return getCommandLineFileResolver(ctx.getRepoDirs());
	}
	
	//// Streamy...stuff
	
	static Consumer<byte[]> outputStreamToConsumer(final OutputStream os) {
		return new Consumer<byte[]>() {
			@Override public void accept(byte[] value) {
				try {
					os.write(value);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};
	}
	
	public static <R> R run(Action<Consumer<byte[]>, R> cmdlet, OutputStream os) throws IOException, InterruptedException {
		try {
			return cmdlet.execute(outputStreamToConsumer(os));
		} finally {
			os.flush();
		}
	}
	
	////
	
	public static boolean isHelpArgument( String arg ) {
		return
			"-?".equals(arg) || "-h".equals(arg) ||
			"--help".equals(arg) || "-help".equals(arg) || "-halp".equals(arg);
	}
	
	public static String USAGE =
		"Usage: ccouch3 <subcommand> <subcommand-arguments>\n" +
		"Version: "+Versions.CCOUCH_VERSION+"\n" +
		"Subcommands:\n" +
		"  identify          ; identify files/directories\n" +
		"  upload            ; upload files to a remote repository\n" +
		"  cache             ; download and cache files from remote repositories\n" +
		"  cat               ; output blobs to standard output\n" +
		"  config            ; dump configuration to stdout\n" +
		"  copy              ; copy files/blobs/directories\n" +
		"  backup            ; back up files locally (simplified implementation)\n" +
		"  command-server    ; run a command server\n" +
		"  web-server        ; run a web server\n" +
		"  find-files        ; find files in local repo for named objects\n"+
		"  store-stream      ; store files or pipe contents\n"+
		"  annotate-m3u      ; add #URN lines to an M3U file\n"+
		"  walk-fs           ; walk filesystem and output basic info\n" +
		"  <subcommand> -?   ; get help on a specific command\n"+
		"\n"+
		"'backup' exists primarily because its implementation is simpler\n"+
		"and easier to debug than that of 'upload';\n"+
		"Otherwise they serve similar purposes.\n";
	
	public static int main( Iterator<String> argi ) throws Exception {
		CCouchContext ctx = new CCouchContext();
		while( argi.hasNext() ) {
			String cmd = argi.next();
			// TODO: Update commands to take general options
			if( ctx.handleCommandLineOption(cmd, argi) ) {
			} else if( "upload".equals(cmd) ) {
				return FlowUploader.uploadMain(ctx, argi);
			} else if( "cache".equals(cmd) ) {
				return Downloader.main(ctx, argi);
			} else if( "cat".equals(cmd) ) {
				return Cat.main(ctx, argi);
			} else if( "copy".equals(cmd) ) {
				return Copy.main(ctx, argi);
			} else if( "config".equals(cmd) ) {
				return ConfigDump.main(ctx, argi);
			} else if( "backup".equals(cmd) ) {
				return UpBacker.backupMain(ctx, argi);
			} else if( "find-files".equals(cmd) ) {
				return FindFilesCommand.main(ctx, argi);
			} else if( "store-stream".equals(cmd) ) {
				return StoreStream.main(ctx, argi);
			} else if( "id".equals(cmd) || "identify".equals(cmd) ) {
				return FlowUploader.identifyMain(ctx, argi);
			} else if( "cmd-server".equals(cmd) || "command-server".equals(cmd) ) {
				return CmdServer.main(ctx, argi);
			} else if( "walk-fs".equals(cmd) ) {
				return WalkFilesystemCmd.main(argi);
			} else if( "web-server".equals(cmd) || "ws".equals(cmd) || "webserv".equals(cmd) ) {
				return WebServerCommand.main(ctx, argi);
			} else if( "verify-tree".equals(cmd) ) {
				return TreeVerifier.main(ctx, argi);
			} else if( "extract-urns".equals(cmd) ) {
				return BlobReferenceScanner.main(ctx, argi);
			} else if( "annotate-m3u".equals(cmd) ) {
				return M3UAnnotator.main(ctx, argi);
			} else if( "help".equals(cmd) || isHelpArgument(cmd) ) {
				System.out.print(USAGE);
				return 0;
			} else if( "--version".equals(cmd) || "-version".equals(cmd) ) {
				System.out.println("ContentCouch "+Versions.CCOUCH_VERSION);
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
