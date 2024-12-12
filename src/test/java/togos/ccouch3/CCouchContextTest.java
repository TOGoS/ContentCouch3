package togos.ccouch3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import togos.ccouch3.CCouchContext.RepoSpec;
import togos.ccouch3.CCouchContext.RepoSpec.RepoType;
import togos.ccouch3.util.ParseResult;

public class CCouchContextTest extends TestCase
{
	protected static <V extends CommandLineOptionHandler<V>>
	ParseResult<List<String>,V> parseAll(List<String> input, CommandLineOptionHandler<V> handler) {
		while( true ) {
			ParseResult<List<String>,V> res = handler.handleCommandLineOption(input);
			if( res.remainingInput != input ) {
				input = res.remainingInput;
				handler = res.result;
				continue;
			}
			return res;
		}
	}
	
	public void testParseRepoExpectingWrongType() {
		ParseResult<List<String>,CCouchContext> res = parseAll(Arrays.asList("-repo:hello","foo.org"), new CCouchContext());
		assertEquals(Collections.emptyList(), res.remainingInput);
		assertFalse(
			new CCouchContext().withPrimaryRepo(new RepoSpec("hello", RepoType.HTTP_N2R, "foo.org")).equals(res.result)
		);
	}
	
	public void testParseRepo() {
		ParseResult<List<String>,CCouchContext> res = parseAll(Arrays.asList("-repo:hello","foo.org"), new CCouchContext());
		assertEquals(Collections.emptyList(), res.remainingInput);
		assertEquals(
			new CCouchContext().withPrimaryRepo(new RepoSpec("hello", RepoType.FILESYSTEM, "foo.org")),
			res.result
		);
	}
	
	public void testParseAnonymousRemoteRepo() {
		ParseResult<List<String>,CCouchContext> res = parseAll(Arrays.asList("-remote-repo","foo.org"), new CCouchContext());
		assertEquals(Collections.emptyList(), res.remainingInput);
		assertEquals(
			new CCouchContext().withAdditionalRemoteRepo(new RepoSpec(null, RepoType.HTTP_N2R, "http://foo.org/uri-res/N2R?")),
			res.result
		);
	}
	
	public void testParseNamedRemoteRepo() {
		ParseResult<List<String>,CCouchContext> res = parseAll(Arrays.asList("-remote-repo:foo","foo.org"), new CCouchContext());
		assertEquals(Collections.emptyList(), res.remainingInput);
		assertEquals(
			new CCouchContext().withAdditionalRemoteRepo(new RepoSpec("foo", RepoType.HTTP_N2R, "http://foo.org/uri-res/N2R?")),
			res.result
		);
	}
	
	public void testParseAll() {
		ParseResult<List<String>,CCouchContext> res = parseAll(Arrays.asList(
			"-sector","stuff",
			"-repo:hello","foo.org",
			"-remote-repo:fred","fred.org",
			"-local-repo:jim","J:/jim/.ccouch",
			"-remote-repo:ted","ted.org",
			"-local-repo:tim","T:/tim/.ccouch"
		), new CCouchContext());
		assertEquals(Collections.emptyList(), res.remainingInput);
		assertEquals(
			new CCouchContext()
				.withStoreSector("stuff")
				.withPrimaryRepo(new RepoSpec("hello", RepoType.FILESYSTEM, "foo.org"))
				.withAdditionalLocalRepo(new RepoSpec("jim", RepoType.FILESYSTEM, "J:/jim/.ccouch"))
				.withAdditionalLocalRepo(new RepoSpec("tim", RepoType.FILESYSTEM, "T:/tim/.ccouch"))
				.withAdditionalRemoteRepo(new RepoSpec("fred", RepoType.HTTP_N2R, "http://fred.org/uri-res/N2R?"))
				.withAdditionalRemoteRepo(new RepoSpec("ted", RepoType.HTTP_N2R, "http://ted.org/uri-res/N2R?"))
			,
			res.result
		);
	}
}
