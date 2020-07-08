package togos.ccouch3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Iterator;

import togos.blob.ByteChunk;
import togos.ccouch3.cmdstream.CmdReader;
import togos.ccouch3.cmdstream.CmdWriter;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.StoreException;
import togos.ccouch3.util.FileUtil;

public class CmdServer
{
	protected final CmdReader r;
	protected final CmdWriter w;
	protected final Repository repo;
	protected final File headDir;
	protected final OutputStream incomingLogStream;
	
	public CmdServer( CmdReader r, CmdWriter w, Repository repo, File headDir, OutputStream incomingLogStream ) {
		this.r = r;
		this.w = w;
		this.repo = repo;
		this.headDir = headDir;
		this.incomingLogStream = incomingLogStream;
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
	 * - Print stuff back to the client:
	 *   echo ...
	 *   -> ok echo ...
	 *
	 * - Head (think HTTP 'HEAD'): Does the specified blob exist on the server?
	 *   head <reqid> <urn>
	 *   -> ok head <reqid> <urn> {found|missing}
	 *
	 * - Upload blobs to the server:
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
	 * - Link a named CCouch head to a blob:
	 *   put <reqid> ccouch-head:<headname> by-urn <hashurn>
	 *   -> ok put <reqid> ccouch-head:<headname> accepted
	 *   or
	 *   -> error put <reqid> could-not-load-by-urn <hashurn>
	 *   or
	 *   -> error put <reqid> destination-is-a-directory <headname>
	 *   or
	 *   -> error put <reqid> destination-exists-and-is-different <headname>
	 *
	 * - Disconnect politely (unceremonious disconnects are also fine):
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
		
		try {
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
			} else if( "post".equals(cmdName) && cmd.length == 5 && "chunk".equals(cmd[3]) ) {
				String urn = cmd[2];
				if( urn.equals("incoming-log") ) {
					int z;
					byte[] buffer = new byte[65536];
					InputStream in = r.getChunkInputStream();
					boolean needsNewline = false;
					while( (z = in.read(buffer)) > 0 ) {
						incomingLogStream.write( buffer, 0, z );
						needsNewline = buffer[z-1] != '\n';
					}
					if( needsNewline ) {
						incomingLogStream.write('\n');
					}
					incomingLogStream.write('\n');
					incomingLogStream.flush();
				}
				w.writeCmd( new String[] { "ok", "post", reqId, urn, "accepted" } );
			} else if( "put".equals(cmdName) && cmd.length == 5 && "chunk".equals(cmd[3]) ) {
				String urn = cmd[2];
				try {
					repo.put( urn, r.getChunkInputStream() );
					w.writeCmd( new String[] { "ok", "put", reqId, urn, "accepted" } );
				} catch( StoreException e ) {
					w.writeCmd( new String[] { "error", "put", reqId, urn, "rejected", tokenize(e.getMessage()) } );
				}
				return true;
			} else if( "put".equals(cmdName) && cmd.length == 5 && cmd[2].startsWith("ccouch-head:") && "by-urn".equals(cmd[3]) ) {
				String headName = cmd[2].substring(12);
				String headUrn = cmd[4];
				ByteChunk headData = repo.getChunk( headUrn, 4096 );
				if( headData == null ) {
					w.writeCmd( new String[] { "error", "put", reqId, cmd[2], "could-not-load-by-urn", headUrn } );
					return true;
				}
				File newHeadFile = new File( headDir + "/" + headName );
				if( newHeadFile.isDirectory() ) {
					w.writeCmd( new String[] { "error", "put", reqId, cmd[2], "destination-is-a-directory", headName } );
					return true;
				}
				FileUtil.mkParentDirs( newHeadFile );
				if( newHeadFile.exists() ) {
					if( !headData.equals(FileUtil.read(newHeadFile)) ) {
						w.writeCmd( new String[] { "error", "put", reqId, cmd[2], "destination-exists-and-is-different", headName } );
						return true;
					}
				} else {
					FileUtil.writeAtomic( newHeadFile, headData );
				}
				w.writeCmd( new String[] { "ok", "put", reqId, cmd[2], "accepted" } );
			} else {
				w.writeCmd( new String[] { "error", cmdName, reqId, "unrecognised-command" } );
			}
			return true;
		} finally {
			w.flush();
		}
	}
	
	public void run() {
		String[] cmd;
		try {
			while( (cmd = r.readCmd()) != null && handleCmd(cmd) );
		} catch( IOException e ) {
			throw new RuntimeException(e);
		}
	}
	
	public static final String USAGE =
		"Usage: ccouch3 cmd-server [options]\n" +
		"\n" +
		"Handles commands and incoming data from 'ccouch3 upload'.\n" +
		"\n" +
		"Options:\n" +
		"  -repo <path>   ; path to repo in which to store blobs, caches, and logs.\n" +
		"  -sector <name> ; name of sector in which to store incoming data";
	
	public static int main(CCouchContext ctx, Iterator<String> argi ) throws Exception {
		for( ; argi.hasNext(); ) {
			String arg = argi.next();
			if( ctx.handleCommandLineOption(arg,  argi)) {
			} else if( CCouch3Command.isHelpArgument(arg) ) {
				System.out.println( USAGE );
				return 0;
			} else {
				System.err.println( "Error: Unrecognised argument: " + arg );
				System.err.println( USAGE );
				return 1;
			}
		}
		if( ctx.storeSector == null ) ctx.storeSector = "cmd-server";
		
		ctx.fix();
		
		Repository repo = ctx.getPrimaryRepository();
		File repoDir = ctx.getPrimaryRepoDir();
		File incomingLogFile = new File(repoDir, "/log/incoming.log");
		FileUtil.mkParentDirs(incomingLogFile);
		FileOutputStream incomingLogStream = new FileOutputStream(incomingLogFile, true);
		CmdServer cs = new CmdServer(new CmdReader(System.in), new CmdWriter(System.out), repo, new File(repoDir, "/heads"), incomingLogStream);
		cs.run();
		incomingLogStream.close();
		return 0;
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit(CmdServer.main( new CCouchContext(), Arrays.asList(args).iterator() ));
	}
}
