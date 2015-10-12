package togos.ccouch3;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import togos.blob.file.FileBlob;

public class CCouchHeadResolver implements BlobResolver
{
	public final Pattern INTPAT = Pattern.compile("^\\d+$");
	public final String HEAD_URN_PREFIX = "x-ccouch-head:";
	protected final File[] headRoots;
	public CCouchHeadResolver( File[] headRoots ) {
		this.headRoots = headRoots;
	}
	
	protected FileBlob findLatest(String dirName) throws FileNotFoundException {
		long latestNumber = -1;
		FileBlob latest = null;
		Matcher intMatcher = INTPAT.matcher("");
		for( File headRoot : headRoots ) {
			File dir = new File(headRoot, dirName);
			String[] entries = dir.list();
			if( entries == null ) continue;
			
			for( String e : entries ) {
				if( intMatcher.reset(e).matches() ) {
					long v = Long.parseLong(e);
					if( v > latestNumber ) {
						latestNumber = v;
						latest = new FileBlob(dir, e);
					}
				}
			}
		}
		
		if( latest == null ) throw new FileNotFoundException("Couldn't find head list '"+dirName+"'");
		return latest;
	}
	
	@Override
	public FileBlob getBlob(String name) throws FileNotFoundException {
		if( !name.startsWith(HEAD_URN_PREFIX) ) {
			throw new FileNotFoundException(getClass().getName()+" only finds "+HEAD_URN_PREFIX+"s!  Not '"+name+"'.");
		}
		
		name = name.substring(HEAD_URN_PREFIX.length());
		if( name.startsWith("//") ) {
			throw new FileNotFoundException(getClass().getName()+" doesn't support heads from specific repositories.");
		}
		
		if( name.startsWith("/") ) name = name.substring(1);
		
		if( name.endsWith("/latest") ) {
			return findLatest(name.substring(0, name.length()-"/latest".length()));
		}
		
		for( File headRoot : headRoots ) {
			FileBlob f = new FileBlob(headRoot, name);
			if( f.exists() ) return f;
		}
		
		throw new FileNotFoundException("Couldn't find head '"+name+"'");
	}

}
