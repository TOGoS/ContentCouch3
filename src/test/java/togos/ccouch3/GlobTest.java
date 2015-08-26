package togos.ccouch3;

import java.io.File;

import junit.framework.TestCase;

public class GlobTest extends TestCase
{
	public static Glob load( File relativeTo, String[] lines ) {
		assert relativeTo != null;
		
		Glob glob = Glob.NUL;
		for( String line : lines ) {
			line = line.trim();
			if( line.startsWith("#") || line.isEmpty() ) continue;
			glob = Glob.parseGlobPattern(relativeTo, line, glob);
		}
		return glob;
	}
	
	public void testMatching() {
		File root = new File("C:/somewhere");
		Glob g = load(root, new String[] {
			"# foo",
			"!baz.html", // Wrong order!
			"*.html",
			"!bar.html",
			"qux",
			"!/qux",
			"/bam/**/roo",
			"/bam/**/blech*/poo",
		});
		
		assertFalse(g.anyMatch(new File(root, "# foo"))); // That line was a comment!
		
		assertFalse(g.anyMatch(new File(root, "bar.html")));
		assertTrue(g.anyMatch(new File(root, "something.html")));
		assertTrue(g.anyMatch(new File(root, "baz.html")));
		
		assertFalse(g.anyMatch(new File(root, "qux")));
		assertTrue(g.anyMatch(new File(root, "stuff/qux")));
		
		assertFalse(g.anyMatch(new File(root, "roo")));
		assertFalse(g.anyMatch(new File(root, "stuff/roo")));
		assertTrue(g.anyMatch(new File(root, "bam/roo")));
		assertTrue(g.anyMatch(new File(root, "bam/stuff/roo")));
		
		assertFalse(g.anyMatch(new File(root, "poo")));
		assertFalse(g.anyMatch(new File(root, "stuff/poo")));
		assertFalse(g.anyMatch(new File(root, "bam/poo")));
		assertFalse(g.anyMatch(new File(root, "bam/stuff/poo")));
		assertTrue(g.anyMatch(new File(root, "bam/stuff/blech/poo")));
		assertTrue(g.anyMatch(new File(root, "bam/stuff/blech.dir/poo")));
		assertFalse(g.anyMatch(new File(root, "bam/stuff/blech.dir/anotherdir/poo")));
	}
}
