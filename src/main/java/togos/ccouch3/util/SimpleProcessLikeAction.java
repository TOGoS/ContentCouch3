package togos.ccouch3.util;

import java.io.IOException;

/**
 * An action that can run itself any number of times, returning an exit code.
 */
public interface SimpleProcessLikeAction {
	public int run() throws IOException, InterruptedException;
}
