package togos.ccouch3;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import togos.ccouch3.hash.BitprintDigest;
import togos.ccouch3.util.Charsets;
import togos.ccouch3.util.Consumer;
import togos.ccouch3.util.StreamingCmdlet;

/**
 * Simple tool for walking the filesystem and collecting
 * various information about files and directories.
 * 
 * Maybe could be extended to walk RDF directories,
 * but I haven't bothered, yet
 * 
 * It would be nice if this had a formatting option.
 * See FindFilesCommand for some preliminary and sloppy
 * work on a formatting system, not to mention sloppy,
 * experimental groundwork for defining these commands
 * in such a way that they can be piped together.
 */
public class WalkFilesystemCmd
implements StreamingCmdlet<Object,Integer> // Object = FileInfo | Exception
{
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
	
	static Pattern NODOTFILES = Pattern.compile("^[^.].*");
	static Pattern ALLFILES = Pattern.compile("^.*");
	
	final static boolean beChatty = true;
	
	final boolean includeDirs = true;
	final boolean includeFiles = true;
	final boolean recurse = true;
	final float extraErrorChance;
	final Random rand = new Random(new Date().getTime());
	final Pattern namePattern;
	final List<Pair<String,File>> roots;
	
	public WalkFilesystemCmd(
		List<Pair<String,File>> roots,
		Pattern namePattern,
		float extraErrorChance
	) {
		this.roots = roots;
		this.namePattern = namePattern;
		this.extraErrorChance = extraErrorChance;
	}
	
	protected String getFileKey(File f) throws IOException, InterruptedException {
		if( extraErrorChance > 0 && rand.nextFloat() < extraErrorChance ) {
			throw new IOException("Synthetic IOException when calculating file key");
		}
		
		ProcessBuilder pb = new ProcessBuilder();
		String[] cmd = new String[] { "fsutil","file","queryfileid",f.getPath() };
		pb.command(cmd);
		pb.redirectOutput(Redirect.PIPE);
		Process p = pb.start();
		p.getOutputStream().close();
		ByteArrayOutputStream collector = new ByteArrayOutputStream();
		InputStream is = p.getInputStream();
		try {
			pipe(is, collector);
		} finally {
			is.close();
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
		if( extraErrorChance > 0 && rand.nextFloat() < extraErrorChance ) {
			throw new IOException("Synthetic IOException when calculating bitprint");
		}
		
		BitprintDigest digest = new BitprintDigest();
		FileInputStream fis = new FileInputStream(f);
		try {
			int z;
			byte[] buf = new byte[65536];
			while( (z=fis.read(buf)) > 0 ) {
				digest.update(buf, 0, z);
			}
			return BitprintDigest.formatUrn(digest.digest());
		} finally {
			fis.close();
		}
	}
	
	static class FileInfo {
		static enum FileType {
			FILE("file"),
			DIRECTORY("dir");
			
			final String code;
			FileType(String code) {
				this.code = code;
			}
			@Override public String toString() { return code; }
		};
		public final String path;
		public final FileType fileType;
		public final long size;
		public final long mtime;
		public final String fileKey;
		public final String bitprint;
		public final List<String> errorMessages;
		public FileInfo(String path, FileType fileType, long size, long mtime, String fileKey, String bitprint, List<String> errorMessages) {
			this.path = path;
			this.fileType = fileType;
			this.size = size;
			this.mtime = mtime;
			this.fileKey = fileKey;
			this.bitprint = bitprint;
			this.errorMessages = errorMessages;
		}
	}
	
	static <T> List<T> add(List<T> l, T item) {
		if( l == Collections.EMPTY_LIST ) l = new ArrayList<T>();
		l.add(item);
		return l;
	}
	
	protected int walk(File f, String asName, Consumer<Object> dest) throws InterruptedException {
		boolean isDir = f.isDirectory();
		FileInfo.FileType fileType = isDir ? FileInfo.FileType.DIRECTORY : FileInfo.FileType.FILE;
		boolean include = isDir ? includeDirs : includeFiles;
		int errorCount = 0;
		List<String> errorMessages = new ArrayList<String>();
		
		if( include ) {
			String bitprint;
			String fileKey;
			
			long size = f.length();
			// Note: 0 might be returned from f.length() in some cases that I
			// might want to distinguish from actually-a-zero-length-file.
			
			long mtime = f.lastModified();
			
			try {
				bitprint = isDir ? null : getFileBitprint(f);
			} catch( IOException e ) {
				add(errorMessages, "Failed to get bitprint for "+f+": "+e.getMessage());
				bitprint = "ERROR";
				++errorCount;
			}
			
			try {
				fileKey = getFileKey(f); 
			} catch (IOException e) {
				add(errorMessages, "Failed to get fileKey for "+f+": "+e.getMessage());
				fileKey = "ERROR";
				++errorCount;
			}
			
			dest.accept(new FileInfo(asName, fileType, size, mtime, fileKey, bitprint, errorMessages));
		}
		
		if( f.isDirectory() && recurse ) {
			File[] files = f.listFiles();
			if( files == null ) {
				++errorCount;
				dest.accept(new IOException("Failed to read entries from "+f.getPath()));
			} else for( File fil : files ) {
				if( namePattern.matcher(fil.getName()).matches() ) {
					errorCount += walk(fil, asName+"/"+fil.getName(), dest);
				}
			}
		}
		
		return errorCount;
	}
	
	@Override public Integer run(Consumer<Object> dest) throws InterruptedException {
		int errorCount = 0;
		for( Pair<String,File> root : roots ) {
			errorCount += walk(root.right, root.left, dest);
		}
		return errorCount;
	}
	
	static Pattern ROOT_PATTERN = Pattern.compile("([^=]+)=(.*)");
	static Pattern EXTRA_ERROR_CHANCE_PATTERN = Pattern.compile("--extra-error-chance=(\\d+(?:\\.\\d+)?)");
	
	static class Pair<A,B> {
		public final A left;
		public final B right;
		Pair(A left, B right) {
			this.left = left;
			this.right = right;
		}
	}
	
	static final String HELP_TEXT =
		"Usage: walk-fs <options> [<alias>=]<file|dir> ...\n" +
		"\n"+
		"Walks the filesystem and outputs basic information about files\n"+
		"\n"+
		"Options:\n"+
		"  --extra-error-chance=0.123 ; chance per file of emitting fake errors\n"+
		"  --ignore-dot-files\n"+
		"  --include-dot-files\n";
	
	public static final DateFormat DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	
	public static StreamingCmdlet<byte[],Integer> parse(Iterator<String> argi) {
		final ArrayList<Pair<String,File>> roots = new ArrayList<Pair<String,File>>();
		boolean parseMode = true;
		boolean includeDotFiles = false;
		float extraErrorChance = 0;
		while( argi.hasNext() ) {
			String arg = argi.next();
			if( parseMode ) {
				Matcher m;
				if( (m = EXTRA_ERROR_CHANCE_PATTERN.matcher(arg)).matches() ) {
					extraErrorChance = Float.parseFloat(m.group(1));
				} else if( !arg.startsWith("-") ) {
					if( (m = ROOT_PATTERN.matcher(arg)).matches() ) {
						roots.add(new Pair<String,File>(m.group(1), new File(m.group(2))));
					} else {
						roots.add(new Pair<String,File>(arg, new File(arg)));
					}
				} else if( "--include-dot-files".equals(arg) ) {
					includeDotFiles = true;
				} else if( "--ignore-dot-files".equals(arg) ) {
					includeDotFiles = false;
				} else if( "--".equals(arg) ) {
					parseMode = false;
				} else if( CCouch3Command.isHelpArgument(arg) ) {
					return new StreamingCmdlet<byte[],Integer>() {
						@Override public Integer run(Consumer<byte[]> dest) throws IOException, InterruptedException {
							dest.accept(HELP_TEXT.getBytes(Charsets.UTF8));
							return 0;
						}
					};
				} else {
					System.err.println("Unrecognized argument: "+arg);
					System.exit(1);
				}
			} else {
				roots.add(new Pair<String,File>(arg, new File(arg)));
			}
		}
		
		final float _extraErrorChance = extraErrorChance;
		final boolean _includeDotFiles = includeDotFiles;
		
		return new StreamingCmdlet<byte[], Integer>() {
			@Override
			public Integer run(final Consumer<byte[]> dest) throws IOException, InterruptedException {
				final String walkerName = WalkFilesystemCmd.class.getSimpleName()+"#walk";
				Date startDate = new Date();
				
				if( beChatty ) {
					dest.accept(("# "+walkerName+" starting at "+DATEFORMAT.format(startDate)+"\n").getBytes(Charsets.UTF8));
				}
				dest.accept("#COLUMNS:path\ttype\tsize\tmtime\tfilekey\tbitprinturn\n".getBytes(Charsets.UTF8));
				
				final Consumer<Object> fileInfoDest = new Consumer<Object>() {
					void error(String message) {
						dest.accept(("# "+message.replace("\n", "\n#  ")+"\n").getBytes(Charsets.UTF8));
					}
					
					@Override
					public void accept(Object item) {
						if( item instanceof FileInfo ) {
							FileInfo fi = (FileInfo)item;
							for( String errorMessage : fi.errorMessages ) error(errorMessage);
							String formatted = fi.path+"\t"+fi.fileType+"\t"+fi.size+"\t"+DATEFORMAT.format(fi.mtime)+"\t"+fi.fileKey+"\t"+fi.bitprint+"\n";
							dest.accept(formatted.getBytes(Charsets.UTF8));
						} else if( item instanceof Exception ) {
							error(((Exception)item).getMessage());
						} else {
							throw new RuntimeException("Unexpected output from "+walkerName+": "+item);
						}
					}
				};
				
				int errorCount = new WalkFilesystemCmd(roots, _includeDotFiles ? ALLFILES : NODOTFILES, _extraErrorChance).run(fileInfoDest);
				
				if( beChatty ) {
					Date endDate = new Date();
					dest.accept((
						"# "+walkerName+" finished at "+DATEFORMAT.format(startDate)+"\n"+
						"# Processing took "+(endDate.getTime()-startDate.getTime())/1000+" seconds\n"+
						"# There were "+errorCount+" errors\n"
					).getBytes(Charsets.UTF8));
				}
				
				return errorCount == 0 ? 0 : 1;
			}
		};
	}
	
	public static int main(Iterator<String> argi) throws IOException, InterruptedException {
		return CCouch3Command.run( parse(argi), System.out );
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		System.exit(main(Arrays.asList(args).iterator()));
	}
}
