package togos.ccouch3;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;

public class CCouch3Command
{
	//// Default repository resolution
	
	public static File resolveRepoDir(String path) {
		if( path.startsWith("~/") ) {
			return new File(System.getProperty("user.home"), path.substring(2));
		} else {
			return new File(path);
		}
	}
	
	public static File getDefaultRepositoryDir() {
		String home = System.getProperty("user.home");
		if( home != null ) {
			return new File(home, ".ccouch");
		}
		return new File(".ccouch");
	}
	
	public static Repository getDefaultRepository(String storeSector) {
		return new SHA1FileRepository(getDefaultRepositoryDir(), storeSector);
	}
	
	public static Repository getDefaultRepository() {
		return getDefaultRepository(null);
	}
	
	public static Repository[] getDefaultRepositories() {
		return new Repository[] { getDefaultRepository() };
	}
	
	public static LiberalFileResolver getCommandLineFileResolver(Repository[] repos) {
		return new LiberalFileResolver(repos);
	}
	
	public static LiberalFileResolver getCommandLineFileResolver() {
		return getCommandLineFileResolver(getDefaultRepositories());
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
		"  copy              ; copy files\n" +
		"  backup            ; back up files locally\n" +
		"  command-server    ; run a command server\n" +
		"  web-server        ; run a web server\n" +
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
		} else if( "web-server".equals(cmd) || "ws".equals(cmd) || "webserv".equals(cmd) ) {
			return WebServerCommand.main(argi);
		} else if( "verify-tree".equals(cmd) ) {
			return TreeVerifier.main(argi);
		} else if( "extract-urns".equals(cmd) ) {
			return BlobReferenceScanner.main(argi);
		} else if( "annotate-m3u".equals(cmd) ) {
			return M3UAnnotator.main(argi);
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
