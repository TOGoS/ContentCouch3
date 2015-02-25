package togos.ccouch3.util;

import java.util.regex.Pattern;

public class RepoURLDefuzzer
{
	protected static final Pattern BARE_HOSTNAME_REPO_PATTERN = Pattern.compile("^[^/]+$");
	protected static final Pattern BARE_HTTP_HOSTNAME_REPO_PATTERN = Pattern.compile("^https?://[^/]+$");
	public static String defuzzRemoteRepoPrefix( String url ) {
		if( BARE_HOSTNAME_REPO_PATTERN.matcher(url).matches() ) {
			url = "http://" + url;
		}
		if( BARE_HTTP_HOSTNAME_REPO_PATTERN.matcher(url).matches() ) {
			url += "/uri-res/N2R?";
		}
		if( !url.endsWith("/") && !url.endsWith("?") ) {
			url += "?";
		}
		return url;
	}
}
