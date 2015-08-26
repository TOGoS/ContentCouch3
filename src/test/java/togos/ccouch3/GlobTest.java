package togos.ccouch3;

import java.io.File;

import junit.framework.TestCase;

public class GlobTest extends TestCase
{
	public void testMatching() {
		File root = new File("C:/somewhere");
		Glob g = Glob.load(root, new String[] {
			"# foo",
			"!baz.html", // Wrong order!
			"*.html",
			"!bar.html",
			".farm*",
			"qux",
			"!/qux",
			"/bam/**/roo",
			"/bam/**/blech*/poo",
		}, null);
		
		assertFalse(Glob.anyMatch(g, new File(root, "# foo"))); // That line was a comment!
		
		assertTrue(Glob.anyMatch(g, new File(root, ".farmy")));
		assertFalse(Glob.anyMatch(g, new File(root, "dfarmy")));

		assertFalse(Glob.anyMatch(g, new File(root, "bar.html")));
		assertTrue(Glob.anyMatch(g, new File(root, "something.html")));
		assertTrue(Glob.anyMatch(g, new File(root, "baz.html")));
		
		assertFalse(Glob.anyMatch(g, new File(root, "qux")));
		assertTrue(Glob.anyMatch(g, new File(root, "stuff/qux")));
		
		assertFalse(Glob.anyMatch(g, new File(root, "roo")));
		assertFalse(Glob.anyMatch(g, new File(root, "stuff/roo")));
		assertTrue(Glob.anyMatch(g, new File(root, "bam/roo")));
		assertTrue(Glob.anyMatch(g, new File(root, "bam/stuff/roo")));
		
		assertFalse(Glob.anyMatch(g, new File(root, "poo")));
		assertFalse(Glob.anyMatch(g, new File(root, "stuff/poo")));
		assertFalse(Glob.anyMatch(g, new File(root, "bam/poo")));
		assertFalse(Glob.anyMatch(g, new File(root, "bam/stuff/poo")));
		assertTrue(Glob.anyMatch(g, new File(root, "bam/stuff/blech/poo")));
		assertTrue(Glob.anyMatch(g, new File(root, "bam/stuff/blech.dir/poo")));
		assertFalse(Glob.anyMatch(g, new File(root, "bam/stuff/blech.dir/anotherdir/poo")));
	}
}
