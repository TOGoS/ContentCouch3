package togos.ccouch3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import togos.blob.ByteBlob;
import togos.blob.file.FileBlob;
import togos.ccouch3.hash.BitprintDigest;
import togos.ccouch3.hash.StreamURNifier;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.util.ListUtil;
import togos.ccouch3.util.ParseResult;
import togos.ccouch3.util.RepoURLDefuzzer;

public class M3UAnnotator
{
	protected static final String SHORT_USAGE =
		"Usage: ccouch3 annotate-m3u [<options>] [<file> ...]\n"+
		"Options:\n"+
		"  -?                     ; print extended help text to stdout and exit\n"+
		"  -transform <transform> ; indicate path transformation (default 'none')\n"+
		"  -tidy-filenames        ; tidy up filenames (spaces to underscores, etc)\n"+
		"                         ; when generating filename hints from paths\n"+
		"  -strict                ; Exit(1) if any entries cannot be annotated.\n";
	protected static final String USAGE =
		SHORT_USAGE+
		"\n"+
		"Path transformation specifications:\n"+
		"  none - paths are left as they are\n"+
		"  raw:<server> - URLs of the form <server>/uri-res/raw/<urn>/<filename>\n"+
		"  N2R:<server> - URLs of the form <server>/uri-res/N2R?<urn>\n"+
		"  local-repo   - Paths to files in the local repository\n"+
		"\n"+
		"Default mode of operation is to emit a #URN:<urn> line for each\n"+
		"file referenced from the M3U.\n"+
		"\n"+
		"If the input file contains #URN: lines, those will be used as the URN\n"+
		"for the following file.  Otherwise the file will be read and its hash\n"+
		"calculated.  If the file cannot be found, the entry is passed through\n"+
		"unmodified and a warning printed to stderr.  If -strict is passed, this\n"+
		"situation will also result in a non-zero exit code.\n";
	
	enum PathTransform {
		NONE,
		RAW,
		N2R,
		LOCAL_REPO
	}
	
	protected static final String URN_LINE_PREFIX = "#URN:";
	
	protected static String basename( String path ) {
		int idx = Math.max(path.lastIndexOf("/"), path.lastIndexOf("\\"));
		return idx == -1 ? path : path.substring(idx+1);
	}
	
	protected static final Pattern URI_PATTERN = Pattern.compile(
		"^" +
		"[A-Za-z][A-Za-z0-9\\+\\.\\-]+" + // The 'scheme part'
		":" +
		"([A-Za-z0-9\\.\\-_~:/\\?#\\[\\]@!\\$&'\\(\\)\\*\\+,;=]|%[A-Fa-f0-9]{2})+"+ // Scheme-specific stuff
		"$");
	
	protected static final boolean isUri( String filenameOrUri ) {
		return URI_PATTERN.matcher(filenameOrUri).matches();
	}
	
	public static int main(CCouchContext ctx, List<String> args) {
		PathTransform pathTransform = PathTransform.NONE;
		String defaultN2rPrefix = null;
		boolean tidyFilenames = false;
		int missingFileCount = 0;
		boolean strict = false;
		
		ArrayList<String> m3uPaths = new ArrayList<String>();
		while( !args.isEmpty() ) {
			ParseResult<List<String>,CCouchContext> ctxPr = ctx.handleCommandLineOption(args);
			if( ctxPr.remainingInput != args ) {
				args = ctxPr.remainingInput;
				ctx  = ctxPr.result;
				continue;
			}
			
			String arg = ListUtil.car(args);
			args = ListUtil.cdr(args);
			if( "-".equals(arg) || !arg.startsWith("-") ) {
				m3uPaths.add(arg);
			} else if( "-strict".equals(arg) ) {
				strict = true;
			} else if( "-tidy-filenames".equals(arg) ) {
				tidyFilenames = true;
			} else if( "-transform".equals(arg) ) {
				String transformSpec = ListUtil.car(args);
				args = ListUtil.cdr(args);
				
				if( "none".equals(transformSpec) ) {
					pathTransform = PathTransform.NONE;
				} else if( transformSpec.startsWith("raw:") ) {
					pathTransform = PathTransform.RAW;
					defaultN2rPrefix = RepoURLDefuzzer.defuzzRemoteRepoPrefix(transformSpec.substring(4));
					defaultN2rPrefix = defaultN2rPrefix.replace("N2R?", "raw/");
				} else if( transformSpec.startsWith("N2R:") ) {
					pathTransform = PathTransform.N2R;
					defaultN2rPrefix = RepoURLDefuzzer.defuzzRemoteRepoPrefix(transformSpec.substring(4));
				} else if( transformSpec.startsWith("local-repo") ) {
					pathTransform = PathTransform.LOCAL_REPO;
				} else {
					System.err.println("Unrecognized path transform: "+transformSpec);
					return 1;
				}
			} else if( CCouch3Command.isHelpArgument(arg) ) {
				System.out.print(USAGE);
				return 0;
			} else {
				System.err.println("Unrecognized argument: "+arg);
				System.err.print(SHORT_USAGE);
				return 1;
			}
		}
		
		ctx = ctx.fixed();
		if( m3uPaths.size() == 0 ) m3uPaths.add("-");
		
		final BlobResolver argumentResolver = CCouch3Command.getCommandLineFileResolver(ctx);
		final BlobResolver entryResolver = new BlobResolver() {
			@Override
			public ByteBlob getBlob(String name) throws IOException {
				FileBlob f = new FileBlob(name);
				if( f.exists() ) return f;
				throw new FileNotFoundException(name);
			}
		};
		String homeDir = System.getProperty("user.home");
		if( homeDir == null ) homeDir = ".";
		final File repoDir = new File(homeDir+"/.ccouch");
		final SLFHashCache hashCache = new SLFHashCache(new File(repoDir,"cache/flow-uploader"));
		final StreamURNifier digestor = BitprintDigest.STREAM_URNIFIER;
		
		class Identifier {
			public String identify( ByteBlob b ) throws IOException {
				String urn;
				
				if( b instanceof File ) {
					try {
						urn = hashCache.getFileUrn((File)b);
					} catch( Exception e ) {
						System.err.println("Warning: Attempt to read hashCache file throw exception: "+e.getMessage());
						urn = null;
					}
					if( urn != null ) return urn;
				}
				
				final InputStream is = b.openInputStream();
				try {
					urn = digestor.digest(is);
				} finally {
					is.close();
				}
				
				if( b instanceof File ) {
					hashCache.cacheFileUrn((File)b, urn);
				}
				
				return urn;
			}
			
			public String identifyResource( String name ) throws IOException {
				return identify(entryResolver.getBlob(name));
			}
		}
		final Identifier identifier = new Identifier();
		
		final Repository[] localRepos = ctx.getLocalRepositories();
		
		try {
			for( String p : m3uPaths ) {
				String providedUrn = null;
				ByteBlob b = argumentResolver.getBlob(p);
				BufferedReader br = new BufferedReader(new InputStreamReader(b.openInputStream()));
				try {
					String line;
					while( (line = br.readLine()) != null ) {
						String t = line.trim();
						if( t.length() > 0 && !t.startsWith("#") ) {
							String urn = providedUrn;
							if( urn == null ) {
								try {
									urn = identifier.identifyResource(t);
								} catch( FileNotFoundException e ) {
									System.err.println("Couldn't find file to identify it: "+t);
									++missingFileCount;
								}
							}
							String path;
						resolvePath:
							switch( pathTransform ) {
							case NONE:
								path = line;
								break;
							case RAW:
								if( urn == null ) {
									path = line;
									break;
								}
								String name = basename(t);
								if( isUri(t) ) name = URLDecoder.decode(name, "UTF-8");
								if( tidyFilenames ) {
									name = name.replace(" (", "-");
									name = name.replace(") ", "-");
									name = name.replace(" - ", "-");
									name = name.replace(" ", "_");
									name = name.replace("&", "+");
									name = name.replaceAll("[^a-zA-Z0-9_+.-]", "");
								}
								path = defaultN2rPrefix+urn+"/"+URLEncoder.encode(name, "UTF-8").replace("+", "%20");
								break;
							case N2R:
								if( urn == null ) {
									path = line;
									break;
								}
								path = defaultN2rPrefix+urn;
								break;
							case LOCAL_REPO:
								for( Repository r : localRepos ) {
									try {
										File f = r.getFile(urn);
										if( f != null ) {
											path = f.getPath();
											break resolvePath;
										}
									} catch( IOException e ) {
									}
								}
								path = line;
								if( new File(path).exists() ) {
									path = line;
									break resolvePath;
								}
								{
									String msg = path + " does not exist on the local file system";
									if( urn != null ) msg += " and "+urn+" not found in any local repository";
									System.err.println(msg);
									++missingFileCount;
								}
								break;
							default:
								throw new RuntimeException("Unrecognized path transform enum value: "+pathTransform);
							}
							
							if( urn != null ) System.out.println(URN_LINE_PREFIX+urn);
							System.out.println(path);
							providedUrn = null;
						} else {
							if( line.startsWith(URN_LINE_PREFIX) ) {
								providedUrn = line.substring(URN_LINE_PREFIX.length()).trim();
							} else {
								// Everything else, just copy it through.
								System.out.println(line);
							}
						}
					}
				} finally {
					br.close();
				}
			}
		} catch( IOException e ) {
			e.printStackTrace(System.err);
			return 1;
		}
		if( missingFileCount > 0 ) {
			System.err.println(missingFileCount+" file(s) couln't be found.");
			System.err.println("Entries for missing files were passed through with no transformations.");
			if( strict ) return 1;
		}
		return 0;
	}
}
