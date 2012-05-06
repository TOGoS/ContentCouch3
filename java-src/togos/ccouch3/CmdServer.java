package togos.ccouch3;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import togos.ccouch3.cmdstream.CmdReader;
import togos.ccouch3.cmdstream.CmdWriter;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;
import togos.ccouch3.repo.StoreException;

public class CmdServer
{
	protected final CmdReader r;
	protected final CmdWriter w;
	protected final Repository repo;
	
	public CmdServer( CmdReader r, CmdWriter w, Repository repo ) {
		this.r = r;
		this.w = w;
		this.repo = repo;
	}
	
	/*
	 * General command syntax:
	 *
	 * <command-name> <request-id> <arg> ...
	 * request-id is an arbitrary string determined by the client
	 * Responses are always of the form:
	 *   ok <command-name> <request-id> ...
	 * or
	 *   error <command-name> <request-id> ...
	 *   
	 * Commands:
	 * 
	 *   echo ...
	 *   -> ok echo ...
	 * 
	 *   head <reqid> <urn>
	 *   -> ok head <reqid> <urn> {found|missing}
	 *
	 *   put <reqid> <urn> chunk <length>
	 *   chunk <length>
	 *   <length bytes of chunk data>
	 *   chunk <length>
	 *   <length bytes of chunk data>
	 *   ...
	 *   end-chunks
	 *   -> ok put <reqid> <urn> accepted
	 *   or
	 *   -> error put <reqid> <urn> rejected <reason>
	 *   
	 *   bye [<reqid>]
	 *   -> ok bye <reqid or "null">
	 *      server closes connection
	 */
	
	protected static String tokenize( String text ) {
		// Might want to take out punctuation and stuff, too... 
		return text.toLowerCase().replace(' ','-');
	}
	
	protected boolean handleCmd( String[] cmd ) throws IOException {
		String cmdName = cmd[0];
		String reqId = cmd.length >= 2 ? cmd[1] : "null";
		
		if( "bye".equals(cmdName) ) {
			w.writeCmd( new String[] { "ok","bye",reqId,"and","farewell" } );
			w.close();
			return false;
		} else if( "echo".equals(cmdName) ) {
			int tokenCount = Math.max(0, cmd.length - 2);
			String[] res = new String[tokenCount+3];
			res[0] = "ok"; res[1] = "echo"; res[2] = reqId;
			for( int i=0; i<tokenCount; ++i ) res[i+3] = cmd[i+2];
			w.writeCmd( res );
			return true;
		} else if( "head".equals(cmdName) && cmd.length == 3 ) {
			String urn = cmd[2];
			w.writeCmd( new String[] { "ok", "head", reqId, urn, repo.contains(urn) ? "found" : "missing" } );
		} else if( "put".equals(cmdName) && cmd.length == 5 && "chunk".equals(cmd[3]) ) {
			String urn = cmd[2];
			try {
				repo.put( urn, r.getChunkInputStream() );
				w.writeCmd( new String[] { "ok", "put", reqId, urn, "accepted" } );
			} catch( StoreException e ) {
				w.writeCmd( new String[] { "error", "put", reqId, urn, "rejected", tokenize(e.getMessage()) } );
			}
			return true;
		} else {
			w.writeCmd( new String[] { "error", cmdName, reqId, "unrecognised-command" } );
		}
		return true;
	}
	
	public void run() {
		String[] cmd;
		try {
			while( (cmd = r.readCmd()) != null && handleCmd(cmd) );
		} catch( IOException e ) {
			throw new RuntimeException(e);
		}
	}
	
	public static int main( Iterator<String> argi ) throws Exception {
		String homeDir = System.getProperty("user.home");
		if( homeDir == null ) homeDir = ".";
		String repoDir = homeDir + "/.ccouch";
		String sector = "cmd-server";
		for( ; argi.hasNext(); ) {
			String arg = argi.next();
			if( "-repo".equals(arg) ) {
				repoDir = argi.next();
			} else if( "-sector".equals(arg) ) {
				sector = argi.next();
			} else {
				throw new RuntimeException("Error: Unrecognised cmd-server argument: " + arg);
			}
		}
		Repository repo = new SHA1FileRepository( new File(repoDir + "/data"), sector);
		CmdServer cs = new CmdServer(new CmdReader(System.in), new CmdWriter(System.out), repo);
		cs.run();
		return 0;
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit(CmdServer.main( Arrays.asList(args).iterator() ));
	}
}
