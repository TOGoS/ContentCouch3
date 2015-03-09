package togos.ccouch3;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import togos.blob.ByteBlob;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;
import togos.ccouch3.util.StreamUtil;

public class Copy
{
	public static String USAGE =
		"Usage: ccouch3 copy [-repo <path]* <source> <destination>";
	
	public static int main(Iterator<String> argi) {
		String fromName = null;
		String toName = null;
		ArrayList<String> repoPaths = new ArrayList<String>();
		
		while( argi.hasNext() ) {
			String arg = argi.next();
			if( "-repo".equals(arg) ) {
				repoPaths.add(0, argi.next());
			} else if( CCouch3Command.isHelpArgument(arg) ) {
				System.out.println(USAGE);
				return 0;
			} else if( !arg.startsWith("-") || "-".equals(arg) ) {
				if( fromName == null ) {
					fromName = arg;
				} else if( toName == null ) {
					toName = arg;
				} else {
					System.err.println("Error: You can only specify 2 files.  Gave: '"+fromName+"', '"+toName+"', '"+arg+"'");
					return 1;
				}
			} else {
				System.err.println("Error: Unrecognized argument: '"+arg+"'");
				return 1;
			}
		}
		
		if( fromName == null || toName == null ) {
			System.err.println("Error: You must specify both source and destination.");
			return 1;
		}
		
		Filesystem fs = new LocalFilesystem("");
		// TODO: Unify repository initialization somewhere
		Collection<Repository> repos = new ArrayList<Repository>();
		for( String repoPath : repoPaths ) {
			repos.add(new SHA1FileRepository(new File(repoPath, "data"), null));
		}
		
		try {
			ByteBlob from = null;
			if( "-".equals(fromName) ) {
				from = new ByteBlob() {
					@Override public long getSize() { return -1; }
					@Override public InputStream openInputStream() throws IOException {
						return System.in;
					}
					@Override public ByteBlob slice(long offset, long length) {
						throw new UnsupportedOperationException();
					}
				};
			} else if( fromName.startsWith("urn:") ) {
				findBlob: for( Repository repo : repos ) {
					from = repo.getBlob(fromName);
					if( from != null ) break findBlob;
				}
			} else {
				from = fs.getBlob(fromName);
			}
			
			if( from == null ) {
				System.err.println("Couldn't find input file '"+fromName+'"');
			}
			
			if( "-".equals(toName) ) {
				InputStream is = from.openInputStream();
				try {
					StreamUtil.copy(is, System.out);
				} finally {
					is.close();
				}
				return 0;
			}
			
			Filesystem toFs = fs;
			String toPath = toName;
			toFs.putBlob(toPath, from, -1);
		} catch( IOException e ) {
			e.printStackTrace();
			return 1;
		}
		
		return 0;
	}
}
