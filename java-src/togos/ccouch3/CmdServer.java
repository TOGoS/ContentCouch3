package togos.ccouch3;

import java.io.IOException;

import togos.ccouch3.cmdstream.CmdReader;
import togos.ccouch3.cmdstream.CmdWriter;
import togos.ccouch3.repo.Repository;
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
	 * Responses are of the form:
	 *   ok <request-id> <result> ...
	 *
	 * Commands:
	 * 
	 *   echo <reqid> <token>
	 *   -> ok <reqid> echo <token>
	 * 
	 *   head <reqid> <urn>
	 *   -> ok <reqid> head <urn> {found|missing}
	 *
	 *   put <reqid> <urn> chunk <length>
	 *   chunk <length>
	 *   <length bytes of chunk data>
	 *   chunk <length>
	 *   <length bytes of chunk data>
	 *   ...
	 *   end-chunks
	 *   -> ok <reqid> put <urn> accepted
	 *   or
	 *   -> error <reqid> put <urn> rejected <reason>
	 *   
	 *   bye [<reqid>]
	 *   -> ok <reqid or "null"> goodbye
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
			w.writeCmd( new String[] { "ok",reqId,"goodbye" } );
			w.close();
			return false;
		} else if( "echo".equals(cmdName) ) {
			String token = cmd.length >= 3 ? cmd[2] : "null";
			w.writeCmd( new String[] { "ok",reqId,"echo",token });
			return true;
		} else if( "head".equals(cmdName) && cmd.length == 3 ) {
			String urn = cmd[2];
			w.writeCmd( new String[] { "ok", reqId, "head", urn, repo.contains(urn) ? "found" : "missing" } );
		} else if( "put".equals(cmdName) && cmd.length == 5 ) {
			String urn = cmd[2];
			try {
				repo.put( urn, r.getChunkInputStream() );
				w.writeCmd( new String[] { "ok", reqId, "put", urn, "accepted" } );
			} catch( StoreException e ) {
				w.writeCmd( new String[] { "error", reqId, "put", urn, "rejected", tokenize(e.getMessage()) } );
			}
			return true;
		} else {
			w.writeCmd( new String[] { "error", reqId, "unrecognised-command", cmdName } );
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
}
