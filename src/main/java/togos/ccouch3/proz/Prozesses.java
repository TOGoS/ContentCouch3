package togos.ccouch3.proz;

import java.io.IOException;

import togos.ccouch3.proz.Prozess.AsyncProzess;
import togos.ccouch3.proz.Prozess.ProtoProzess;
import togos.ccouch3.util.Charsets;

public class Prozesses {
	private Prozesses() { throw new RuntimeException("Don't instantiate me, bro!"); }
	
	public abstract static class RunnableProzessImpl<C,R> implements Prozess<C,R>, Prozess.RunnableProzess<C,R> {
		@Override public <X> X visit(ProzessHandler<C,R,X> handler) { return handler.accept(this); }
	}
	public static abstract class AsyncProzessImpl<C,R> implements Prozess<C,R>, AsyncProzess<C,R> {
		@Override public <X> X visit(ProzessHandler<C,R,X> handler) { return handler.accept(this); }
	}
	public static abstract class ProtoProzessImpl<C,R> implements Prozess<C,R>, ProtoProzess<C,R> {
		@Override public <X> X visit(ProzessHandler<C,R,X> handler) { return handler.accept(this); }
	}
	public static class CompletedProzessImpl<C,R> implements Prozess<C,R> {
		final R result;
		public CompletedProzessImpl(R result) {
			this.result = result;
		}
		@Override public <X> X visit(ProzessHandler<C,R,X> handler) { return handler.acceptResult(this.result); }
	}
	
	static final Integer ZERO = Integer.valueOf(0);
	static final CompletedProzessImpl<?,Integer> DONE = new CompletedProzessImpl<Void,Integer>(ZERO);
	@SuppressWarnings("unchecked")
	public static <C,R> Prozess<C,R> done(R result) {
		return ZERO.equals(result) ? (CompletedProzessImpl<C,R>)DONE : new CompletedProzessImpl<C,R>(result);
	}
	public static <C> Prozess<C,Integer> done() { return done(ZERO); }
	
	public static <R> Prozess<SystemContext,R> output(final int fd, final byte[] data, final Prozess<SystemContext,R> next, final Prozess<SystemContext,R> onError) {
		return new ProtoProzessImpl<SystemContext,R>() {
			public @Override Prozess<SystemContext,R> start(final SystemContext ctx) {
				return new RunnableProzessImpl<SystemContext,R>() {
					@Override
					public Prozess<SystemContext,R> run() {
						try {
							ctx.getOutputStream(fd).write(data);
						} catch (IOException e) {
							return onError;
						}
						return next;
					}
				};
			}
		};
	}
	public static <R> Prozess<SystemContext,R> outputAndDone(final int fd, final byte[] data, R result, R errorResult) {
		return output(fd, data, Prozesses.<SystemContext,R>done(result), Prozesses.<SystemContext,R>done(errorResult));
	}
	public static Prozess<SystemContext,Integer> outputAndDone(final int fd, final byte[] data, Integer result) {
		return outputAndDone(fd, data, result, Integer.valueOf(-1));
	}
	public static Prozess<SystemContext,Integer> outputAndDone(final int fd, final String text, Integer result) {
		return outputAndDone(fd, text.getBytes(Charsets.UTF8), result);
	}
	
	/** Combines starting and running so you don't have to write such nested inner classes */
	public static abstract class RunnableWithContextProzessImpl<C,R> extends ProtoProzessImpl<C,R> {
		public abstract Prozess<C,R> run(C ctx) throws Exception;
		public abstract Prozess<C,R> fail(Exception e, C ctx);
		
		@Override public Prozess<C,R> start(final C ctx) {
			return new Prozesses.RunnableProzessImpl<C,R>() {
				@Override
				public Prozess<C,R> run() {
					try {
						return RunnableWithContextProzessImpl.this.run(ctx);
					} catch( Exception e ) {
						return fail(e, ctx);
					}
				}
			};
		}
	}
}
