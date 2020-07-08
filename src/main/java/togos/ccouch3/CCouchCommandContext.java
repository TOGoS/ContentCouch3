package togos.ccouch3;

import java.util.Iterator;

public class CCouchCommandContext
implements CommandLineOptionHandler
{
	// Presumably stuff about I/O will go in here?
	// But verbosity isn't yet being used, hence it being commented-out.
	/*
	static final int VERBOSITY_SILENT = 0;
	static final int VERBOSITY_ERRORS = 100;
	static final int VERBOSITY_WARNINGS = 200;
	static final int VERBOSITY_USAGE_WARNINGS = 300;
	static final int VERBOSITY_DEFAULT = VERBOSITY_USAGE_WARNINGS;
	static final int VERBOSITY_VERBOSE = 400;
	static final int VERBOSITY_VERY_VERBOSE = 500;
	 */
	
	RepoConfig repoConfig = new RepoConfig();
	//int verbosityLevel;
	
	@Override
	public boolean handleCommandLineOption(String arg, Iterator<String> argi ) {
		if( repoConfig.handleCommandLineOption(arg, argi) ) return true;
		
		/*
		if( "-q".equals(arg) ) { verbosityLevel = VERBOSITY_ERRORS; return true; }
		if( "-v".equals(arg) ) { verbosityLevel = VERBOSITY_VERBOSE; return true; }
		if( "-vv".equals(arg) ) { verbosityLevel = VERBOSITY_VERY_VERBOSE; return true; }
		*/
		
		return false;
	}
}
