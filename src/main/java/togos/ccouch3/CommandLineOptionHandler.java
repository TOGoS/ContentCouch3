package togos.ccouch3;

import java.util.List;

import togos.ccouch3.util.ParseResult;

/** Anything that handles command-line options. */
public interface CommandLineOptionHandler<T>
{
	/**
	 * Either handles the argument arg, also slurping any additional arguments needed from rest, returning true,
	 * or ignores it, returning false.
	 *
	 * @return true if the option was understood, false otherwise
	 */
	ParseResult<List<String>,T> handleCommandLineOption(List<String> args);
}
