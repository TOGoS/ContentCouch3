package togos.ccouch3.util;

/**
 * Represents the result of attempting to parse.
 * 
 * remainingInput is some representation of the input being parsed.
 * It might be a string or list of tokens, or it might be an
 * integer representing the position within some shared input buffer.
 * 
 * To indicate failure to parse, it is conventional to
 * return the original input and null result.
 * 
 * Otherwise, semantics are left to the user.
 * 
 * @param <I> some representation of input pointer
 * @param <R> result type
 */
public class ParseResult<I,R> {
	public final I remainingInput;
	public final R result;
	public ParseResult(I remainingInput, R result) {
		this.remainingInput = remainingInput;
		this.result = result;
	}
	
	public static <I,R> ParseResult<I,R> of(I remainingInput, R result) {
		return new ParseResult<I,R>(remainingInput, result);
	}
	
	public static <I,R> ParseResult<I,R> failed(I remainingInput) {
		return new ParseResult<I,R>(remainingInput, null);
	}
}
