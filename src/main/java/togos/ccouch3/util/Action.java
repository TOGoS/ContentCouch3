package togos.ccouch3.util;

import java.io.IOException;

/**
 * Formerly 'StreamingCmdlet',
 * which took a Consumer<T>.
 * Has been made more generic.
 * 
 * Might want to make the throwable exception types more generic, too?
 * 
 * Might want to move to TScript34-P0010.
 */
public interface Action<C,R> {
	public R execute(C context) throws IOException, InterruptedException;
}
