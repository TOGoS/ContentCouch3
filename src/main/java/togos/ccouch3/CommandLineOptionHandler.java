package togos.ccouch3;

import java.util.Iterator;

/** Anything that handles command-line options. */
public interface CommandLineOptionHandler
{
	/**
	 * Either handles the argument arg, also slurping any additional arguments needed from rest, returning true,
	 * or ignores it, returning false.
	 *
	 * @return true if the option was understood, false otherwise
	 */
	boolean handleCommandLineOption(String arg, Iterator<String> rest);
}
