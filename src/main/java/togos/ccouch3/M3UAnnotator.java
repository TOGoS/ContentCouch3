package togos.ccouch3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;

import togos.blob.ByteBlob;
import togos.blob.file.FileBlob;
import togos.ccouch3.hash.BitprintDigest;
import togos.ccouch3.hash.StreamURNifier;
import togos.ccouch3.util.RepoURLDefuzzer;

public class M3UAnnotator
{
	protected static final String SHORT_USAGE =
		"Usage: ccouch3 annotate-m3u [<options>] [<file> ...]\n"+
		"Options:\n"+
		"  -?                     ; print extended help text to stdout and exit\n"+
		"  -transform <transform> ; indicate path transformation (default 'none')\n"+
		"  -tidy-filenames        ; tidy up filenames (spaces to underscores, etc)\n"+
		"                         ; when generating filename hints from paths\n";
	protected static final String USAGE =
		SHORT_USAGE+
		"\n"+
		"Path transformation specifications:\n"+
		"  none - paths are left as they are\n"+
		"  raw:<server> - URLs of the form <server>/uri-res/raw/<urn>/<filename>\n"+
		"\n"+
		"Default mode of operation is to emit a #URN:<urn> line for each\n"+
		"file referenced from the M3U.\n";
	
	enum PathTransform {
		NONE,
		RAW,
		N2R
	}
	
	protected static String basename( String path ) {
		int idx = Math.max(path.lastIndexOf("/"), path.lastIndexOf("\\"));
		return idx == -1 ? path : path.substring(idx+1);
	}
	
	public static int main(Iterator<String> argi) {
		PathTransform pathTransform = PathTransform.NONE;
		String defaultN2rPrefix = null;
		boolean tidyFilenames = false;
		
		ArrayList<String> m3uPaths = new ArrayList<String>();
		while( argi.hasNext() ) {
			String arg = argi.next();
			if( "-".equals(arg) || !arg.startsWith("-") ) {
				m3uPaths.add(arg);
			} else if( "-tidy-filenames".equals(arg) ) {
				tidyFilenames = true;
			} else if( "-transform".equals(arg) ) {
				String transformSpec = argi.next();
				if( "none".equals(transformSpec) ) {
					pathTransform = PathTransform.NONE;
				} else if( transformSpec.startsWith("raw:") ) {
					pathTransform = PathTransform.RAW;
					defaultN2rPrefix = RepoURLDefuzzer.defuzzRemoteRepoPrefix(transformSpec.substring(4));
					defaultN2rPrefix = defaultN2rPrefix.replace("N2R?", "raw/");
				} else if( transformSpec.startsWith("N2R:") ) {
					pathTransform = PathTransform.N2R;
					defaultN2rPrefix = RepoURLDefuzzer.defuzzRemoteRepoPrefix(transformSpec.substring(4));
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
		
		if( m3uPaths.size() == 0 ) m3uPaths.add("-");
		
		final BlobResolver argumentResolver = CCouch3Command.getCommandLineFileResolver();
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
		
		try {
			for( String p : m3uPaths ) {
				ByteBlob b = argumentResolver.getBlob(p);
				BufferedReader br = new BufferedReader(new InputStreamReader(b.openInputStream()));
				try {
					String line;
					while( (line = br.readLine()) != null ) {
						String t = line.trim();
						if( t.length() > 0 && !t.startsWith("#") ) {
							String urn = identifier.identifyResource(t);
							switch( pathTransform ) {
							case NONE:
								break;
							case RAW:
								String name = basename(t);
								if( tidyFilenames ) {
									name = name.replace(" (", "-");
									name = name.replace(") ", "-");
									name = name.replace(" - ", "-");
									name = name.replace(" ", "_");
									name = name.replace("&", "+");
									name = name.replaceAll("[^a-zA-Z0-9_+.-]", "");
								}
								line = defaultN2rPrefix+urn+"/"+URLEncoder.encode(name, "UTF-8").replace("+", "%20");
								break;
							case N2R:
								line = defaultN2rPrefix+urn;
								break;
							default:
								throw new RuntimeException("Unrecognized path transform enum value: "+pathTransform);
							}
							
							System.out.println("#URN:"+urn);
						}
						System.out.println(line);
					}
				} finally {
					br.close();
				}
			}
		} catch( IOException e ) {
			e.printStackTrace(System.err);
			return 1;
		}
		return 0;
	}
}
