package togos.ccouch3;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import togos.ccouch3.hash.BitprintDigest;
import togos.ccouch3.util.Charsets;

/**
 * Simple tool for walking the filesystem and collecting
 * various information about files and directories.
 */
public class WalkFilesystemCmd {
	static void pipe(InputStream is, OutputStream os) throws IOException {
		byte[] buf = new byte[65536];
		int z;
		while( (z = is.read(buf)) > 0 ) {
			os.write(buf, 0, z);
		}
	}
	
	static final Pattern FSUTIL_QUERYFILEID_OUTPUT_PATTERN = Pattern.compile("^File ID is (.*)$");
	
	static String quoteArgv(String[] args) {
		StringBuilder sb = new StringBuilder();
		String sep = "";
		for( String arg : args ) {
			sb.append(sep);
			sb.append(arg);
			sep = " ";
		}
		return sb.toString();
	}
	
	protected String getFileKey(File f) throws IOException, InterruptedException {
		ProcessBuilder pb = new ProcessBuilder();
		String[] cmd = new String[] { "fsutil","file","queryfileid",f.getPath() };
		pb.command(cmd);
		pb.redirectOutput(Redirect.PIPE);
		Process p = pb.start();
		p.getOutputStream().close();
		ByteArrayOutputStream collector = new ByteArrayOutputStream();
		try(InputStream is = p.getInputStream()) {
			pipe(is, collector);
		}
		int exitCode = p.waitFor();
		if( exitCode != 0 ) {
			throw new IOException("`"+quoteArgv(cmd)+"` exited with code "+exitCode);
		}
		String output = new String(collector.toByteArray(), Charsets.UTF8).trim();
		Matcher m = FSUTIL_QUERYFILEID_OUTPUT_PATTERN.matcher(output);
		if( m.matches() ) return m.group(1);
		throw new IOException("Failed to find file ID in fsutil output: "+output);
	}
	
	public String getFileBitprint(File f) throws IOException {
		BitprintDigest digest = new BitprintDigest();
		try( FileInputStream fis = new FileInputStream(f) ) {
			int z;
			byte[] buf = new byte[65536];
			while( (z=fis.read(buf)) > 0 ) {
				digest.update(buf, 0, z);
			}
			return BitprintDigest.formatUrn(digest.digest());
		}
	}
	
	protected void leaf(File f) {
	}
	
	boolean includeDirs = true;
	boolean includeFiles = true;
	boolean recurse = true;
	Pattern namePattern = Pattern.compile("^[^.].*");
	
	public void walk(File f, String asName) throws InterruptedException {
		boolean isDir = f.isDirectory();
		boolean include = isDir ? includeDirs : includeFiles;
		
		if( include ) {
			String bitprint;
			String fileKey;
			
			try {
				bitprint = isDir ? null : getFileBitprint(f);
			} catch( IOException e ) {
				System.err.print("Failed to get bitprint for "+f+": ");
				bitprint = "ERROR";
			}
			
			try {
				fileKey = getFileKey(f); 
			} catch (IOException e) {
				System.err.print("Failed to get fileKey for "+f+": ");
				e.printStackTrace();
				fileKey = "ERROR";
			}
			
			System.out.println(asName+"\t"+fileKey+"\t"+bitprint);
		}
		
		if( f.isDirectory() && recurse ) {
			File[] files = f.listFiles();
			for( File fil : files ) {
				if( namePattern.matcher(fil.getName()).matches() ) {
					walk(fil, asName+"/"+fil.getName());
				}
			}
		}
	}
	
	static Pattern ROOT_PATTERN = Pattern.compile("([^=]+)=(.*)");
	
	static class Pair<A,B> {
		public final A left;
		public final B right;
		Pair(A left, B right) {
			this.left = left;
			this.right = right;
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		ArrayList<Pair<String,File>> roots = new ArrayList<Pair<String,File>>();
		boolean parseMode = true;
		for( String arg : args ) {
			if( parseMode ) {
				Matcher m;
				if( (m = ROOT_PATTERN.matcher(arg)).matches() ) {
					roots.add(new Pair<String,File>(m.group(1), new File(m.group(2))));
				} else if( !arg.startsWith("-") ) {
					roots.add(new Pair<String,File>(arg, new File(arg)));
				} else if( "--".equals(arg) ) {
					parseMode = false;
				} else {
					System.err.println("Unrecognized argument: "+arg);
					System.exit(1);
				}
			} else {
				roots.add(new Pair<String,File>(arg, new File(arg)));
			}
		}
		
		WalkFilesystemCmd cmd = new WalkFilesystemCmd();
		
		for( Pair<String,File> root : roots ) {
			cmd.walk(root.right, root.left);
		}
	}
}
