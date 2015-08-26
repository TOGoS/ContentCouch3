package togos.ccouch3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;

public class Glob {
	public static final Glob NUL = new Glob(false, Pattern.compile("(?!x)x"), null);
	
	/** For purposes of pattern matching, always use this instead of File.separator */
	public static final String SEPARATOR = "/";
	
	public final boolean negate;
	public final Pattern pattern;
	public final Glob next;
	public Glob( boolean negate, Pattern p, Glob next ) {
		this.negate = negate;
		this.pattern = p;
		this.next = next;
	}
	
	protected static String path( File f ) {
		return f.getPath().replace(File.separator, SEPARATOR);
	}
	
	protected static Glob parseGlobPattern( File relativeTo, String glob, Glob next ) {
		boolean negate;
		if( glob.startsWith("!") ) {
			negate = true;
			glob = glob.substring(1);
		} else {
			negate = false;
		}
		
		String prefix;
		if( glob.startsWith("/") ) {
			prefix = "^"+Pattern.quote(path(relativeTo)+"/");
			glob = glob.substring(1);
		} else {
			prefix = "(?:^|.*/)";
		}
		
		// Based on the * and ** rules used by .gitignore:
		//	https://git-scm.com/docs/gitignore
		
		if( glob.startsWith("**/") ) glob = glob.substring(3);
		
		glob = glob.replace("/**/", "/[ANY DIRECTORIES]");
		glob = glob.replace("**", "[ANYTHING]");
		glob = glob.replace("*", "[ANYTHING BUT A SLASH]");
		
		glob = glob.replace("[ANY DIRECTORIES]", "(?:.*/)*");
		glob = glob.replace("[ANYTHING]", ".*");
		glob = glob.replace("[ANYTHING BUT A SLASH]", "[^/]*");
		
		return new Glob(negate, Pattern.compile(prefix+glob), next);
	}
	
	public boolean anyMatch( File f ) {
		String path = path(f);
		Glob g = this;
		while( g != null ) {
			if( g.pattern.matcher(path).matches() ) {
				return !g.negate;
			}
			g = g.next;
		}
		return false;
	}
	
	public static Glob load( File f ) throws IOException {
		File relativeTo = f.getParentFile();
		assert relativeTo != null;
		
		FileReader fr = new FileReader(f);
		Glob glob = NUL;
		try {
			@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(fr);
			String line;
			while( (line = br.readLine()) != null ) {
				line = line.trim();
				if( line.startsWith("#") || line.isEmpty() ) continue;
				glob = parseGlobPattern(relativeTo, line, glob);
			}
		} finally {
			fr.close();
		}
		return glob;
	}
}
