package togos.ccouch3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import togos.ccouch3.hash.BitprintDigest;
import togos.ccouch3.hash.StreamURNifier;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;
import togos.ccouch3.repo.StoreException;
import togos.ccouch3.util.FileUtil;
import togos.ccouch3.util.ListUtil;
import togos.ccouch3.util.LogUtil;
import togos.ccouch3.util.ParseResult;

public class UpBacker
{
	static class BackupCmd {
		public static final String USAGE =
			"Usage: ccouch3 backup -repo <repo-path> [options] <file1> <file2> ...\n" +
			"\n" +
			"Backs up files locally by explicitly copying them.\n" +
			"To keep maintenance costs low the implementation of this command\n" +
			"favors robustness and simplicity over efficiency.\n" +
			"Compare with 'upload'.\n" +
			"\n" +
			"Options:\n" +
			"  -sector <name> ; indicate section of repo in which to store data\n" +
			"  -show-progress ; note progress on stderr as program runs\n" +
			"  -show-report   ; report to stderr after each root is completed\n" +
			"  -files-only    ; only store file contents, no directories\n" +
			"  -dirs-only     ; only store directory listings (including file references),\n" +
			"                 ; but no file contents\n" +
			"  -omit-file-mtimes ; do not include file modification times in serialized\n"+
			"                 ; directory data\n"+
			"  -?             ; show help and exit\n";
		
		enum Mode {
			RUN, USAGE_ERROR, HELP
		};
		public final Mode mode;
		public final String errorMessage;
		public final UpBacker storer;
		public final List<File> thingsToStore;
		
		public BackupCmd( Mode mode, String errorMessage, UpBacker storer, List<File> thingsToStore ) {
			this.mode = mode;
			this.errorMessage = errorMessage;
			this.storer = storer;
			this.thingsToStore = thingsToStore;
		}
		
		public BackupCmd( UpBacker storer, List<File> thingsToStore ) {
			this( Mode.RUN, null, storer, thingsToStore );
		}
		
		public BackupCmd( Mode mode, String errorMessage ) {
			this( mode, errorMessage, null, null );
		}
		
		public int run() {
			switch( mode ) {
			case RUN:
				return storer.store(thingsToStore);
			case USAGE_ERROR:
				System.err.println("Error: "+errorMessage);
				System.err.println("Run with -? for help.");
				return 1;
			case HELP:
				System.out.print(USAGE);
				return 0;
			default:
				throw new RuntimeException("Invalid mode: "+mode);
			}
		}
	}
	
	public final Repository repo;
	public final File headDir;
	
	public boolean shouldStoreFileContents = true;
	public boolean shouldStoreDirectoryListings = true;
	public boolean shouldShowProgress;
	public boolean shouldReportResults;
	
	protected final StreamURNifier digestor = BitprintDigest.STREAM_URNIFIER;
	protected final NewStyleRDFDirectorySerializer directorySerializer;
	protected final File incomingLogFile;
	protected FileOutputStream incomingLogStream;
	
	public UpBacker( File repoDir, String sector, boolean includeFileMtimes ) {
		this.repo = new SHA1FileRepository( new File(repoDir, "data"), sector );
		this.headDir = new File(repoDir, "heads");
		this.incomingLogFile = new File(repoDir, "log/incoming.log");
		this.directorySerializer = new NewStyleRDFDirectorySerializer(includeFileMtimes);
	}
	
	class StoreResult {
		final int errorCount;
		final int totalObjectCount;
		final int storedObjectCount;
		final FileInfo fileInfo;
		
		public StoreResult( int errorCount, int totalObjectCount, int storedObjectCount, FileInfo fileInfo ) {
			this.errorCount = errorCount;
			this.totalObjectCount = totalObjectCount;
			this.storedObjectCount = storedObjectCount;
			this.fileInfo = fileInfo;
		}
		
		public StoreResult( int errorCount ) {
			this( errorCount, 0, 0, null );
		}
	}
	
	protected FileOutputStream getIncomingLogStream() throws IOException {
		if( incomingLogStream == null ) {
			FileUtil.mkParentDirs(incomingLogFile);
			incomingLogStream = new FileOutputStream(incomingLogFile, true);
		}
		return incomingLogStream;
	}
	
	protected void rootStored( File f, StoreResult sr ) {
		if( sr.storedObjectCount > 0 && sr.fileInfo != null ) {
			String message = LogUtil.formatStorageLogEntry(new Date(), sr.fileInfo.getFsObjectType(), sr.fileInfo.getPath(), sr.fileInfo.getUrn());
			try {
				FileOutputStream log = getIncomingLogStream();
				log.write( (message + "\n\n").getBytes() );
				log.flush();
			} catch( IOException e ) {
				System.err.println("Failed to write to log stream: "+e.getMessage());
			}
		}
		
		if( shouldShowProgress ) {
			showProgress();
		}
		if( shouldShowProgress && shouldReportResults ) {
			if( shouldShowProgress ) hideProgress();
		}
		if( shouldReportResults ) {
			System.err.println(f.getPath()+" -> "+(sr.fileInfo == null ? "(error)" : sr.fileInfo.getUrn()));
			System.err.println("  "+sr.errorCount+" errors, "+sr.totalObjectCount+" objects read, "+sr.storedObjectCount+" objects stored");
		}
	}
	
	String currentPath;
	int objectsRead, objectsStored;
	
	protected void showProgressHeader() {
		System.err.println("    Read;   Stored; Last file found");
	}
	protected void showProgress() {
		String pathTail = currentPath.length() <= 55 ? currentPath : currentPath.substring(currentPath.length()-55);
		System.err.print( String.format("% 8d; % 8d; %-55s\r", objectsRead, objectsStored, pathTail) );
	}
	protected void hideProgress() {
		String s10 = "          ";
		System.err.print( s10+s10+s10+s10+s10+s10+s10+"     \r" );
	}
	
	protected StoreResult store( File f, Glob ignores ) {
		if( shouldShowProgress ) {
			currentPath = f.getPath();
			showProgress();
		}
		
		if( !f.exists() ) {
			System.err.println("Failed to store "+f+"; it doesn't seem to exist");
			return new StoreResult(1);
		}

		if( f.isDirectory() ) {
			File ignoreFile = new File(f, ".ccouchignore");
			try {
				if( ignoreFile.exists() ) ignores = Glob.load(ignoreFile, ignores);
			} catch( IOException e1 ) {
				System.err.println("Error loading ignore file: "+ignoreFile);
			}
			
			File[] subFiles = f.listFiles();
			if( subFiles == null ) {
				System.err.println("Failed to store "+f+"; unable to list directory");
				return new StoreResult(1);
			}
			
			++objectsRead;
			int errorCount = 0, totalCount = 1, storedCount = 0;
			
			ArrayList<DirectoryEntry> entries = new ArrayList<DirectoryEntry>();
			for( File sf : subFiles ) {
				if( FileUtil.shouldIgnore(ignores, sf) ) continue;
				
				StoreResult r = store(sf, ignores);
				errorCount += r.errorCount;
				if( r.fileInfo != null ) {
					entries.add( new DirectoryEntry(sf.getName(), r.fileInfo ) );
				}
				totalCount += r.totalObjectCount;
				storedCount += r.storedObjectCount;
			}
			
			String dirUrn;
			try {
				ByteArrayOutputStream dser = new ByteArrayOutputStream();
				directorySerializer.serialize(entries, dser);
				byte[] data = dser.toByteArray();
				String dirRdfUrn = digestor.digest(new ByteArrayInputStream(data));
				if( this.shouldStoreDirectoryListings && !repo.contains(dirRdfUrn) ) {
					try {
						repo.put(dirRdfUrn, new ByteArrayInputStream(data));
						++storedCount;
						++objectsStored;
					} catch( StoreException e ) {
						++errorCount;
						System.err.println("Failed to store "+f+" serialization "+dirRdfUrn);
					}
				}
				dirUrn = "x-rdf-subject:"+dirRdfUrn;
			} catch( IOException e ) {
				// This shouldn't happen.
				throw new RuntimeException(e);
			}
			
			return new StoreResult(errorCount, totalCount, storedCount, new FileInfo(f.getPath(), dirUrn, FSObjectType.DIRECTORY, f.length(), f.lastModified()));
		}
		
		String fileUrn = null;
		try {
			FileInputStream fis = new FileInputStream( f );
			fileUrn = digestor.digest(fis);
			fis.close();
		} catch( IOException e ) {
			System.err.println("Error reading "+f+": "+e.getMessage());
			return new StoreResult(1);
		}
		
		++objectsRead;
		
		// Note: it is possible for the size to change
		// after it's been digested.  Would be good if the digestor
		// returned the number of bytes read to ensure that the size
		// matches the number of bytes accounted for by fileUrn.
		
		long size = f.length();
		long mtime = f.lastModified();
		int storedCount = 0;
		
		if( this.shouldStoreFileContents && !repo.contains(fileUrn) ) {
			try {
				FileInputStream fis = new FileInputStream( f );
				repo.put(fileUrn, fis);
				fis.close();
				++storedCount;
				++objectsStored;
			} catch( Exception e ) {
				System.err.println("Error storing "+f+": "+e.getMessage());
				return new StoreResult(1);
			}
		}
		
		return new StoreResult(0, 1, storedCount, new FileInfo(f.getPath(), fileUrn, FSObjectType.BLOB, size, mtime));
	}
	
	public int store( List<File> thingsToStore ) {
		if( shouldShowProgress ) showProgressHeader();
		
		try {
			getIncomingLogStream();
		} catch( IOException e ) {
			System.err.println("Failed to open log stream; you may want to address this before continuing!");
			e.printStackTrace();
		}
		
		int errorCount = 0;
		for( File f : thingsToStore ) {
			StoreResult r = store(f, FileUtil.DEFAULT_IGNORES);
			errorCount += r.errorCount;
			rootStored(f, r);
		}
		if( shouldShowProgress ) hideProgress();
		return errorCount;
	}
	
	public static BackupCmd fromArgs(CCouchContext ctx, List<String> args ) {
		ArrayList<String> pathsToStore = new ArrayList<String>();
		String storeSector = "local";
		boolean showReport = false, showProgress = false;
		boolean includeFileMtimes = true;
		boolean shouldStoreFileContents = true;
		boolean shouldStoreDirectoryListings = true;
				
		while( !args.isEmpty() ) {
			ParseResult<List<String>,CCouchContext> ctxPr = ctx.handleCommandLineOption(args);
			if( ctxPr.remainingInput != args ) {
				args = ctxPr.remainingInput;
				ctx  = ctxPr.result;
				continue;
			}
			
			String arg = ListUtil.car(args);
			args = ListUtil.cdr(args);
			if( !arg.startsWith("-") ) {
				pathsToStore.add(arg);
			} else if( "-sector".equals(arg) ) {
				storeSector = ListUtil.car(args);
				args = ListUtil.cdr(args);
			} else if( "-files-only".equals(arg) ) {
				shouldStoreFileContents = true;
				shouldStoreDirectoryListings = false;
			} else if( "-dirs-only".equals(arg) ) {
				shouldStoreFileContents = false;
				shouldStoreDirectoryListings = true;
			} else if( "-show-progress".equals(arg) ) {
				showProgress = true;
			} else if( "-omit-file-mtimes".equals(arg) ) {
				includeFileMtimes = false;
			} else if( "-show-report".equals(arg) || "-v".equals(arg) ) {
				showReport = true;
			} else if( CCouch3Command.isHelpArgument(arg) ) {
				return new BackupCmd(BackupCmd.Mode.HELP, null);
			} else {
				return new BackupCmd(BackupCmd.Mode.USAGE_ERROR, "Unrecognized argument: "+arg);
			}
		}
		
		ctx = ctx.fixed(); // I SHOULDN'T HAVE TO REMEMBER TO DO THIS; STINKY CODE!!!
		if( ctx.primaryRepo == null ) {
			return new BackupCmd(BackupCmd.Mode.USAGE_ERROR, "No -repo specified");
		}
		
		UpBacker upBacker = new UpBacker( ctx.primaryRepo.getDirectory(), storeSector, includeFileMtimes );
		upBacker.shouldStoreFileContents = shouldStoreFileContents;
		upBacker.shouldStoreDirectoryListings = shouldStoreDirectoryListings;
		upBacker.shouldReportResults = showReport;
		upBacker.shouldShowProgress = showProgress;
		
		ArrayList<File> filesToStore = new ArrayList<File>();
		for( String p : pathsToStore ) filesToStore.add(new File(p));
		return new BackupCmd( upBacker, filesToStore );
	}
	
	public static int backupMain(CCouchContext ctx, List<String> args ) {
		return fromArgs(ctx, args).run();
	}
}
