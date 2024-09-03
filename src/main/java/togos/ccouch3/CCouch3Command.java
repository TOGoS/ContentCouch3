package togos.ccouch3;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import togos.ccouch3.proz.SystemContext;
import togos.ccouch3.proz.SystemContextImpl;
import togos.ccouch3.proz.ProzessRunner;
import togos.ccouch3.proz.Prozess;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;
import togos.ccouch3.util.Action;
import togos.ccouch3.util.Consumer;
import togos.ccouch3.util.ListUtil;
import togos.ccouch3.util.ParseResult;
import togos.ccouch3.util.StreamUtil;

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
	
	public static <R> R run(Action<Consumer<byte[]>, R> cmdlet, OutputStream os) throws IOException, InterruptedException {
		try {
			return cmdlet.execute(StreamUtil.outputStreamToConsumer(os));
		} finally {
			os.flush();
		}
	}
	public static int run(Prozess<SystemContext,Integer> cmdlet, OutputStream os) throws IOException, InterruptedException {
		try {
			SystemContext ctx = new SystemContextImpl(
				new File("").getAbsoluteFile(), System.getenv(), new Object[] {
					null, os, System.err
				});
			return ProzessRunner.run(cmdlet, ctx);
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
		"  annotate-m3u      ; add #URN lines to an M3U file\n"+
		"  backup            ; back up files locally (simplified implementation)\n" +
		"  cache             ; download and cache files from remote repositories\n" +
		"  cat               ; output blobs to standard output\n" +
		"  config            ; dump configuration to stdout\n" +
		"  command-server    ; run a command server\n" +
		"  copy              ; copy files/blobs/directories\n" +
		"  find-files        ; find files in local repo for named objects\n"+
		"  identify          ; identify files/directories\n" +
		"  store-stream      ; store files or pipe contents\n"+
		"  upload            ; upload files to a remote repository\n" +
		"  walk-fs           ; walk filesystem and output basic info\n" +
		"  web-server        ; run a web server\n" +
		"  <subcommand> -?   ; get help on a specific command\n"+
		"\n"+
		"There is overlap in functionality between the commands.\n"+
		"e.g. 'backup', 'upload', and 'store-stream' can all store\n"+
		"files, but have different options and implementations.\n"+
		"";
	
	public static int main( List<String> args ) throws Exception {
		CCouchContext ctx = new CCouchContext();
		while( !args.isEmpty() ) {
			ParseResult<List<String>,CCouchContext> ctxPr = ctx.handleCommandLineOption(args);
			if( ctxPr.remainingInput != args ) {
				args = ctxPr.remainingInput;
				ctx  = ctxPr.result;
				continue;
			}
			
			if( args.size() == 0 ) break;
			
			String cmd = ListUtil.car(args);
			args = ListUtil.cdr(args);
			
			if( "upload".equals(cmd) ) {
				return FlowUploader.uploadMain(ctx, args);
			} else if( "cache".equals(cmd) ) {
				return Downloader.main(ctx, args);
			} else if( "cat".equals(cmd) ) {
				return Cat.main(ctx, args);
			} else if( "copy".equals(cmd) ) {
				return Copy.main(ctx, args);
			} else if( "config".equals(cmd) ) {
				return ConfigDump.main(ctx, args);
			} else if( "backup".equals(cmd) ) {
				return UpBacker.backupMain(ctx, args);
			} else if( "find-files".equals(cmd) ) {
				return FindFilesCommand.main(ctx, args);
			} else if( "store-stream".equals(cmd) ) {
				return StoreStream.main(ctx, args);
			} else if( "id".equals(cmd) || "identify".equals(cmd) ) {
				return FlowUploader.identifyMain(ctx, args);
			} else if( "cmd-server".equals(cmd) || "command-server".equals(cmd) ) {
				return CmdServer.main(ctx, args);
			} else if( "walk-fs".equals(cmd) ) {
				return WalkFilesystemCmd.main(args);
			} else if( "web-server".equals(cmd) || "ws".equals(cmd) || "webserv".equals(cmd) ) {
				return WebServerCommand.main(ctx, args);
			} else if( "verify-tree".equals(cmd) ) {
				return TreeVerifier.main(ctx, args);
			} else if( "extract-urns".equals(cmd) ) {
				return BlobReferenceScanner.main(ctx, args);
			} else if( "annotate-m3u".equals(cmd) ) {
				return M3UAnnotator.main(ctx, args);
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
		System.exit( main( Arrays.asList(args)) );
	}
}
