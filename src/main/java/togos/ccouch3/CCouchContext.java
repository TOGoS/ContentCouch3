package togos.ccouch3;

import static togos.ccouch3.util.RepoURLDefuzzer.defuzzRemoteRepoPrefix;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import togos.ccouch3.CCouchContext.RepoSpec.RepoType;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;
import togos.ccouch3.util.ListUtil;
import togos.ccouch3.util.ObjectUtil;
import togos.ccouch3.util.ParseResult;
import togos.ccouch3.util.StringUtil;

/**
 * Context within which all our work is done!
 * Indicates repositories and maybe other stuff.
 */
public class CCouchContext
implements CommandLineOptionHandler<CCouchContext>
{
	/** Basic info about a repository */
	static class RepoSpec {
		enum RepoType {
			FILESYSTEM,
			HTTP_N2R
		}
		
		public final String name;
		public final RepoType type;
		/** File path or HTTP server prefix */
		public final String location;
		
		public RepoSpec( String name, RepoType type, String location ) {
			this.name = name;
			this.type = type;
			this.location = location;
		}
		
		protected String getDescription() {
			String desc = "RepoConfig";
			if( name != null ) desc += " '"+name+"'";
			if( location != null ) desc += " ("+location+")";
			return desc;
		}
		
		public @Override String toString() {
			return "RepoConfig(name="+StringUtil.quote(name)+", type="+type+", location="+StringUtil.quote(location)+")";
		}
		public @Override boolean equals(Object obj) {
			if( !(obj instanceof RepoSpec)) {
				return false;
			}
			RepoSpec ors = (RepoSpec)obj;
			return ObjectUtil.equals(name, ors.name) && ObjectUtil.equals(type, "xxx") && ObjectUtil.equals(location, ors.location);
		}
		public @Override int hashCode() {
			return ObjectUtil.hashCode(name, type, location);
		}
		
		public File getDirectory() {
			String name = "Repo";
			if( this.name != null ) name += " '"+name+"'";
				
			if( this.type != RepoType.FILESYSTEM ) throw new RuntimeException(getDescription()+" is not a local filesystem repo");
			if( this.location == null ) throw new RuntimeException(getDescription()+" has no location specified"); 
			return new File(location);
		}
	}
	
	/** Repository into which to store stuff. */ 
	public final RepoSpec primaryRepo;
	/** Name of sector into which to store stuff. */
	public final String storeSector;
	/**
	 * List of repositories in addition to the primary one
	 * that are considered local
	 * (if we're caching and they contain stuff, nothing needs to be done).
	 */
	public final List<RepoSpec> localRepos;
	/**
	 * List of remote repositories.
	 */
	public final List<RepoSpec> remoteRepos;
	
	public CCouchContext(RepoSpec primaryRepo, String storeSector, List<RepoSpec> localRepos, List<RepoSpec> remoteRepos) {
		this.primaryRepo = primaryRepo;
		this.storeSector = storeSector;
		this.localRepos  = localRepos;
		this.remoteRepos = remoteRepos;
	}
	public CCouchContext() {
		this(null, null, Collections.<RepoSpec>emptyList(), Collections.<RepoSpec>emptyList());
	}
	
	protected static String repoSpecToArglike(String role, RepoSpec rs) {
		if( rs == null ) return "";
		return "--"+role+(rs.name == null ? "" : ":"+rs.name)+"=" + rs.location + " ("+rs.type+")";
	}
	
	public @Override String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("CCouchContext(");
		String sep = "";
		if( this.storeSector != null ) {
			sb.append(sep);
			sb.append("--store-sector="+StringUtil.quote(this.storeSector));
			sep = " ";
		}
		if( this.primaryRepo != null ) {
			sb.append(sep);
			sb.append(repoSpecToArglike("primary-repo", this.primaryRepo));
			sep = " ";
		}
		for( RepoSpec repo : localRepos ) {
			sb.append(sep);
			sb.append(repoSpecToArglike("local-repo", repo));
			sep = " ";
		}
		for( RepoSpec repo : remoteRepos ) {
			sb.append(sep);
			sb.append(repoSpecToArglike("remote-repo", repo));
			sep = " ";
		}
		sb.append(")");
		return sb.toString();
	}
	
	public @Override boolean equals(Object obj) {
		
		return this.toString().equals(obj.toString());
	}
	
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}
	
	
	public CCouchContext withPrimaryRepo(RepoSpec primaryRepo) {
		return new CCouchContext(primaryRepo, storeSector, localRepos, remoteRepos);
	}
	public CCouchContext withStoreSector(String storeSector) {
		return new CCouchContext(primaryRepo, storeSector, localRepos, remoteRepos);
	}
	public CCouchContext withAdditionalLocalRepo(RepoSpec repo) {
		return new CCouchContext(primaryRepo, storeSector, ListUtil.snoc(localRepos, repo), remoteRepos);
	}
	public CCouchContext withAdditionalRemoteRepo(RepoSpec repo) {
		return new CCouchContext(primaryRepo, storeSector, localRepos, ListUtil.snoc(remoteRepos, repo));
	}
	
	protected static String resolveRepoDir(String path) {
		if( path.startsWith("~/") ) {
			return System.getProperty("user.home") + "/" + path.substring(2);
		} else {
			return path;
		}
	}
	
	// TODO: Pass in env instead of using System.env
	protected static String getNonEmptyEnv(String name) {
		String v = System.getenv(name);
		return v == null || v.isEmpty() ? null : v;
	}
	
	protected static RepoSpec getEnvSpecifiedRepository() {
		// 2020-07-07: I have decided that using a default (based on user.home) is PROBLEMATIC
		// after on at least a couple occasions running out of disk space
		// when ccouch cached things to the default location, when I didn't mean for it to!
		
		// 2024-02-24: Switching to UPPERCASE!
		
		String dir = getNonEmptyEnv("CCOUCH_REPO_DIR");
		if( dir == null ) return null;
		return new RepoSpec( getNonEmptyEnv("CCOUCH_REPO_NAME"), RepoType.FILESYSTEM, dir );
	}
	
	static ParseResult<List<String>,String> optArg(String opt, String explicit, List<String> rest) {
		if( explicit != null ) return ParseResult.of(rest, explicit);
		
		if( rest.isEmpty() ) {
			throw new RuntimeException(opt+" requires an argument but wasn't given one");
		}
		
		return ParseResult.of(ListUtil.cdr(rest), ListUtil.car(rest));
	}
	
	@Override
	public ParseResult<List<String>,CCouchContext> handleCommandLineOption(List<String> args) {
		if( args.isEmpty() ) return ParseResult.of(args, this); 
		String opt;
		String explicitArgument = null;
		String rawOpt = ListUtil.car(args);
		if( rawOpt.startsWith("--") ) {
			final int equalIndex = rawOpt.indexOf("=");
			if( equalIndex != -1 ) {
				explicitArgument = rawOpt.substring(equalIndex + 1);
				rawOpt = rawOpt.substring(0, equalIndex);
			}
			opt = rawOpt.substring(2);
		} else if( rawOpt.startsWith("-") ) {
			opt = rawOpt.substring(1);
		} else {
			return ParseResult.of(args, this);
		}
		List<String> args1 = ListUtil.cdr(args);
		
		final String name;
		final int colonIdx = opt.indexOf(':');
		if( colonIdx != -1 ) {
			name = opt.substring(colonIdx+1);
			opt = opt.substring(0, colonIdx);
		} else {
			name = null;
		}
		
		if( "repo".equals(opt) ) {
			ParseResult<List<String>,String> dirPr = optArg(rawOpt, explicitArgument, args1);
			return ParseResult.of(
				dirPr.remainingInput, 
				withPrimaryRepo(new RepoSpec(name, RepoType.FILESYSTEM, resolveRepoDir(dirPr.result)))
			);
		} else if( "local-repo".equals(opt) ) {
			ParseResult<List<String>,String> dirPr = optArg(rawOpt, explicitArgument, args1);
			return ParseResult.of(
				dirPr.remainingInput,
				withAdditionalLocalRepo(new RepoSpec(name, RepoType.FILESYSTEM, resolveRepoDir(dirPr.result)))
			);
		} else if( "remote-repo".equals(opt) ) {
			ParseResult<List<String>,String> uriPr = optArg(rawOpt, explicitArgument, args1);
			return ParseResult.of(
				uriPr.remainingInput,
				withAdditionalRemoteRepo(new RepoSpec(name, RepoType.HTTP_N2R, defuzzRemoteRepoPrefix(uriPr.result)))
			);
		} else if( "sector".equals(opt) ) {
			ParseResult<List<String>,String> storeSectorPr = optArg(rawOpt, explicitArgument, args1);
			return ParseResult.of(
				storeSectorPr.remainingInput,
				withStoreSector(storeSectorPr.result)
			);
		} else {
			return ParseResult.of(args, this);
		}
	}
	
	/**
	 * Should be called after all command-line options have been processed.
	 * Fills in defaults and maybe loads more info about repositories?
	 *
	 * This feels hacky.
	 * Maybe there should be separate config/context classes
	 * so there is a clearer (and typechecked) point of transformation?
	 */
	public CCouchContext fixed() {
		RepoSpec primaryRepo = this.primaryRepo;
		if( primaryRepo == null ) {
			primaryRepo = getEnvSpecifiedRepository();
		}
		String storeSector = this.storeSector;
		if( storeSector == null ) storeSector = "user"; // The traditional default.  But maybe there should be no default idk.
		// TODO: read primaryRepo/remote-repos.lst, or whatever it's called,
		// or any other configuration based on reading files in .ccouch,
		// which maybe actually we don't want to do at all lol
		// (preferring environment variables instead these days)
		return new CCouchContext(primaryRepo, storeSector, localRepos, remoteRepos);
	}
	
	public File getPrimaryRepoDir() {
		if( primaryRepo == null ) {
			throw new RuntimeException("No primary repository configured");
		}
		return primaryRepo.getDirectory();
	}
	public File getPrimaryRepoDir(File defaultValue) {
		if( primaryRepo == null ) return defaultValue;
		return primaryRepo.getDirectory();
	}

	
	public Repository[] getLocalRepositories() {
		File[] dirs = getRepoDirs();
		Repository[] repos = new Repository[dirs.length];
		for( int i=0; i<dirs.length; ++i ) {
			repos[i] = new SHA1FileRepository(new File(dirs[i], "data"),
				// Only give it a storeSector if it's the primary repository
				primaryRepo != null && dirs[i].getPath().equals(primaryRepo.location) ?
					storeSector : null);
		}
		return repos;
	}
	
	public File[] getRepoDirs() {
		LinkedHashSet<String> paths = new LinkedHashSet<String>();
		if( primaryRepo != null ) paths.add(primaryRepo.location);
		for( RepoSpec rs : localRepos ) paths.add(rs.location);
		File[] repoDirs = new File[paths.size()];
		int i=0; for( String p : paths ) {
			repoDirs[i++] = new File(p);
		}
		return repoDirs;
	}
	
	public String[] getRemoteRepoUrls() {
		LinkedHashSet<String> urls = new LinkedHashSet<String>();
		for( RepoSpec rs : localRepos ) urls.add(rs.location);
		return urls.toArray(new String[urls.size()]);
	}

	public SHA1FileRepository getPrimaryRepository() {
		return new SHA1FileRepository(new File(getPrimaryRepoDir(), "data"), storeSector);
	}
}
