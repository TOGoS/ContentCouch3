package togos.ccouch3;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.ProcessBuilder.Redirect;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import togos.ccouch3.WalkFilesystemCmd.WFileInfo.FileType;
import togos.ccouch3.hash.BitprintDigest;
import togos.ccouch3.util.Action;
import togos.ccouch3.util.Charsets;
import togos.ccouch3.util.Consumer;
import togos.ccouch3.util.ListUtil;

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
implements Action<WalkFilesystemCmd.FileInfoConsumer,Integer> // Object = FileInfo | Exception
{
	/**
	 * An object representing a filesystem path
	 * that can create new instances representing
	 * sub-paths
	 */
	interface AppendablePathLike {
		AppendablePathLike sub(String entryName);
		/** Returns the string representation of the path */
		@Override String toString();
	}
	// Maybe it would be better to just store a prefix
	// and a list of path segments and let someone
	// else figure out what to do with them later.
	static class AppendablePathLikeImpl implements AppendablePathLike {
		final String current;
		final String separator;
		final Function<String,String> pathsegEncoder;
		public AppendablePathLikeImpl(
			String current, String separator,
			Function<String,String> pathsegEncoder
		) {
			this.current = current;
			this.separator = separator;
			this.pathsegEncoder = pathsegEncoder;
		}
		@Override
		public AppendablePathLike sub(String entryName) {
			String prefix =
				current.isEmpty() ? "" :
				current.endsWith(this.separator) ? this.current :
				this.current + this.separator;
			return new AppendablePathLikeImpl(
				prefix + pathsegEncoder.apply(entryName),
				this.separator,
				this.pathsegEncoder
			);
		}
		@Override
		public String toString() {
			if( this.current.isEmpty() ) {
				return ".";
			} else {
				return this.current;
			}
		}
	}
	
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
	
	// final static boolean beChatty = true;
	
	final boolean includeDirs = true;
	final boolean includeFiles = true;
	final boolean recurse = true;
	final float extraErrorChance;
	final Random rand = new Random(new Date().getTime());
	final Pattern namePattern;
	final List<Pair<AppendablePathLike,File>> roots;
	
	boolean includeFileKeys = false;
	
	public WalkFilesystemCmd(
		List<Pair<AppendablePathLike,File>> roots,
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
	
	// The 'W' is to differentiate from togos.ccouch3.FileInfo,
	// which is different.
	static class WFileInfo {
		static enum FileType {
			FILE("file"),
			DIRECTORY("dir");
			
			final String code;
			FileType(String code) {
				this.code = code;
			}
			@Override public String toString() { return code; }
		};
		/**
		 * An abstract path-like identifier for the file
		 */
		public final AppendablePathLike path;
		public final FileType fileType;
		public final long size;
		// Modification time, or 0 to indicate unknown
		public final long mtime;
		public final String fileKey;
		public final String bitprint;
		public final List<String> errorMessages;
		public WFileInfo(AppendablePathLike path, FileType fileType, long size, long mtime, String fileKey, String bitprint, List<String> errorMessages) {
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
	
	protected int walk(File f, AppendablePathLike asName, FileInfoConsumer dest) throws InterruptedException {
		boolean isDir = f.isDirectory();
		WFileInfo.FileType fileType = isDir ? WFileInfo.FileType.DIRECTORY : WFileInfo.FileType.FILE;
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
				bitprint = null;
				++errorCount;
			}
			
			if( includeFileKeys ) try {
				fileKey = getFileKey(f); 
			} catch (IOException e) {
				add(errorMessages, "Failed to get fileKey for "+f+": "+e.getMessage());
				fileKey = null;
				++errorCount;
			} else {
				fileKey = null;
			}
			
			dest.fileInfo(new WFileInfo(asName, fileType, size, mtime, fileKey, bitprint, errorMessages));
		}
		
		if( f.isDirectory() && recurse ) {
			File[] files = f.listFiles();
			if( files == null ) {
				++errorCount;
				dest.error(new IOException("Failed to read entries from "+f.getPath()));
			} else for( File fil : files ) {
				String filName = fil.getName();
				if( namePattern.matcher(filName).matches() ) {
					errorCount += walk(fil, asName.sub(filName), dest);
				}
			}
		}
		
		return errorCount;
	}
	
	@Override public Integer execute(FileInfoConsumer dest) throws InterruptedException {
		int errorCount = 0;
		for( Pair<AppendablePathLike,File> root : roots ) {
			errorCount += walk(root.right, root.left, dest);
		}
		return errorCount;
	}
	
	static Pattern ROOT_PATTERN = Pattern.compile("([^=]*)=(.*)");
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
		"  --output-format={tsv|tsv-manifest} ; indicate output format by name\n"+
		"  --extra-error-chance=0.123 ; chance per file of emitting fake errors\n"+
		"  --ignore-dot-files\n"+
		"  --include-dot-files\n";
	
	public static final DateFormat DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	
	public static interface FileInfoConsumer {
		void begin(Date startDate);
		void fileInfo(WFileInfo info);
		void error(Exception error);
		void end(Date endDate, int errorCount);
	}
	
	static abstract class ChattyFileInfoOutputter implements FileInfoConsumer {
		final Consumer<byte[]> dest;
		final String walkerName;
		final boolean beChatty;
		Date startDate;
		public ChattyFileInfoOutputter(
			Consumer<byte[]> dest,
			String walkerName,
			boolean beChatty
		) {
			this.dest = dest;
			this.walkerName = walkerName;
			this.beChatty = beChatty;
		}
		
		@Override public void begin(Date startDate) {
			this.startDate = startDate;
			if( beChatty ) {
				emit("# "+walkerName+" starting at "+DATEFORMAT.format(startDate)+"\n");
			}
		}
		
		
		void emit(String text) {
			dest.accept(text.getBytes(Charsets.UTF8));
		}
		
		void error(String message) {
			emit("# "+message.replace("\n", "\n#  ")+"\n");
		}
		
		@Override public void error(Exception error) {
			error(error.getMessage());
		}
		
		@Override public void end(Date endDate, int errorCount) {
			if( beChatty ) {
				emit(
					"# "+walkerName+" finished at "+DATEFORMAT.format(startDate)+"\n"+
					"# Processing took "+(endDate.getTime()-startDate.getTime())/1000+" seconds\n"+
					"# There were "+errorCount+" errors\n"
				);
			}
		}
	}
	
	static class TSVFileInfoWriter extends ChattyFileInfoOutputter {
		public TSVFileInfoWriter(
			Consumer<byte[]> dest,
			String walkerName,
			boolean beChatty
		) {
			super(dest, walkerName, beChatty);
		}
		
		@Override public void begin(Date startDate) {
			super.begin(startDate);
			emit("#COLUMNS:path\ttype\tsize\tmtime\tfilekey\tbitprinturn\n");
		}
		
		protected String toTsvF(String v) {
			if( v == null ) return "-";
			return v;
		}
		
		@Override public void fileInfo(WFileInfo fi) {
			for( String errorMessage : fi.errorMessages ) error(errorMessage);
			String formatted = fi.path+"\t"+fi.fileType+"\t"+fi.size+"\t"+DATEFORMAT.format(fi.mtime)+"\t"+toTsvF(fi.fileKey)+"\t"+toTsvF(fi.bitprint)+"\n";
			dest.accept(formatted.getBytes(Charsets.UTF8));
		}
	};
	
	static class TSVFMFileInfoWriter extends ChattyFileInfoOutputter {
		public TSVFMFileInfoWriter(
			Consumer<byte[]> dest,
			String walkerName,
			boolean beChatty
		) {
			super(dest, walkerName, beChatty);
		}
		
		@Override public void begin(Date startDate) {
			emit("#format http://ns.nuke24.net/Formats/TSVFileManifestV1\n");
			emit("# (See http://www.nuke24.net/docs/2024/TSVFileManifestV1.html)\n");
			// TODO Auto-generated method stub
			super.begin(startDate);
		}
		
		@Override public void fileInfo(WFileInfo info) {
			if( info.fileType != FileType.FILE ) {
				// This format's not really designed to
				// talk about directories.
				return;
			}
			for( String errorMessage : info.errorMessages ) error(errorMessage);
			
			StringBuilder sb = new StringBuilder();
			
			sb.append(info.path);
			// Always put in the tabs even when some values
			// are blank so you can split on them and get the parts
			// at predictable indexes
			sb.append("\t");
			sb.append(info.bitprint == null ? "" : info.bitprint);
			sb.append("\t");
			
			String sep = "";
			if( info.mtime != 0 ) {
				sb.append(sep).append(": dc:modified @ ").append(info.mtime);
				sep = " ";
			}
			// Assuming for now that info.size is always meaningful;
			// empty files are a thing and special-case leaving it out
			// might confuse some reader of this file.
			sb.append(sep).append(": bz:fileLength @ ").append(info.size);
			sep = " ";
			if( info.fileKey != null ) {
				sb.append(sep).append(": ccouch:fileNodeId @ \"" + info.fileKey + "\"");
				sep = " ";
			}
			sb.append("\n");
			
			emit(sb.toString());
		}
	}
	
	static class URIEncoder implements Function<String,String> {
		static final URIEncoder instance = new URIEncoder();
		private URIEncoder() { }
		@Override public String apply(String input) {
			try {
				return URLEncoder.encode(input, "UTF-8").replace("+", "%20");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
	}
	static class NoEncoder implements Function<String,String> {
		static final NoEncoder instance = new NoEncoder();
		private NoEncoder() { }
		@Override public String apply(String input) { return input; }
	}
	
	static enum OutputFormat {
		TSV,
		TSVFileManifest
	};
	
	static FileInfoConsumer makeOutputter(OutputFormat format, Consumer<byte[]> dest, String walkerName, boolean beChatty) {
		switch( format ) {
		case TSV: return new TSVFileInfoWriter(dest, walkerName, beChatty);
		case TSVFileManifest: return new TSVFMFileInfoWriter(dest, walkerName, beChatty);
		default: throw new RuntimeException("Bad format: "+format);
		}
	}
	
	public static Action<Consumer<byte[]>,Integer> parse(List<String> args) {
		final ArrayList<Pair<String,File>> roots = new ArrayList<Pair<String,File>>();
		boolean parseMode = true;
		boolean includeDotFiles = false;
		boolean beChatty = true;
		boolean includeFileKeys = false;
		float extraErrorChance = 0;
		OutputFormat outputFormat = OutputFormat.TSVFileManifest;
		while( !args.isEmpty() ) {
			String arg = ListUtil.car(args);
			args = ListUtil.cdr(args);
			if( parseMode ) {
				Matcher m;
				if( (m = EXTRA_ERROR_CHANCE_PATTERN.matcher(arg)).matches() ) {
					extraErrorChance = Float.parseFloat(m.group(1));
				} else if( !arg.startsWith("-") ) {
					if( (m = ROOT_PATTERN.matcher(arg)).matches() ) {
						roots.add(new Pair<String,File>(m.group(1), new File(m.group(2))));
					} else {
						roots.add(new Pair<String,File>(null, new File(arg)));
					}
				} else if( "--include-dot-files".equals(arg) ) {
					includeDotFiles = true;
				} else if( "--ignore-dot-files".equals(arg) ) {
					includeDotFiles = false;
				} else if( "--include-file-keys".equals(arg) ) {
					includeFileKeys = true;
				} else if( "--output-format=tsv-manifest".equals(arg) ) {
					outputFormat = OutputFormat.TSVFileManifest;
				} else if( "--output-format=tsv".equals(arg) ) {
					outputFormat = OutputFormat.TSV;
				} else if( "--".equals(arg) ) {
					parseMode = false;
				} else if( CCouch3Command.isHelpArgument(arg) ) {
					return new Action<Consumer<byte[]>,Integer>() {
						@Override public Integer execute(Consumer<byte[]> dest) throws IOException, InterruptedException {
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
		final boolean _beChatty = beChatty;
		final boolean _includeFileKeys = includeFileKeys;
		final OutputFormat _outputFormat = outputFormat;
		
		return new Action<Consumer<byte[]>, Integer>() {
			@Override
			public Integer execute(final Consumer<byte[]> dest) throws IOException, InterruptedException {
				final String walkerName = WalkFilesystemCmd.class.getSimpleName()+"#walk (ContentCouch"+Versions.CCOUCH_VERSION+")";
				
				List<Pair<AppendablePathLike,File>> roots1 = new ArrayList<Pair<AppendablePathLike,File>>();
				Function<String,String> pathsegEncoder = _outputFormat == OutputFormat.TSV ? NoEncoder.instance : URIEncoder.instance;
				for( Pair<String,File> root : roots ) {
					String prefix = root.left;
					if( prefix == null ) prefix = pathsegEncoder.apply(root.right.getName());
					roots1.add(new Pair<AppendablePathLike,File>(
						new AppendablePathLikeImpl(prefix, "/", pathsegEncoder),
						root.right
					));
				}
				
				WalkFilesystemCmd walker = new WalkFilesystemCmd(roots1, _includeDotFiles ? ALLFILES : NODOTFILES, _extraErrorChance);
				walker.includeFileKeys = _includeFileKeys;
				
				FileInfoConsumer fileInfoDest = makeOutputter(_outputFormat, dest, walkerName, _beChatty);
				fileInfoDest.begin(new Date());
				int errorCount = walker.execute(fileInfoDest);
				fileInfoDest.end(new Date(), errorCount);
				
				return errorCount == 0 ? 0 : 1;
			}
		};
	}
	
	public static int main(List<String> args) throws IOException, InterruptedException {
		return CCouch3Command.run( parse(args), System.out );
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		System.exit(main(Arrays.asList(args)));
	}
}
