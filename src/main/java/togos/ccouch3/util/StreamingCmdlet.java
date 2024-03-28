package togos.ccouch3.util;

import java.io.IOException;

/**
 * A 'commandlet' whose main side-effects
 * are outputting to the provided stream and
 * returning an exit code.
 */
public interface StreamingCmdlet<T,R> {
	public R run(Consumer<T> dest) throws IOException, InterruptedException;
}
