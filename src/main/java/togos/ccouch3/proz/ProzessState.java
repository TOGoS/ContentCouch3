package togos.ccouch3.proz;

/**
 * Experimental interface representing a
 * process in one of various forms:
 * 
 * - Something like an actual system process, which can be started
 *   with some I/O configuration and other context,
 *   runs asynchronously for a while, and then can
 *   be waited-for
 * - Something that can be run synchronously
 * - Something like a Danducer that is side-effect free
 *   and needs to have its states stepped-through.
 * */
public interface ProzessState {
	public abstract class RunnableProzessState implements ProzessState {
		public abstract ProzessState run();
		@Override public <X> X visit(ProzessHandler<X> handler) { return handler.accept(this); }
	}
	public abstract class AsyncProzessState implements ProzessState {
		public abstract void abort();
		public abstract ProzessState waitFor() throws InterruptedException;
		@Override public <X> X visit(ProzessHandler<X> handler) { return handler.accept(this); }
	}
	public abstract class ProtoProzessState implements ProzessState {
		public abstract ProzessState start(ProzessContext ctx);
		@Override public <X> X visit(ProzessHandler<X> handler) { return handler.accept(this); }
	}
	// Functional versions also possible; add when necessary
	public class CompletedProzessState implements ProzessState {
		final int result;
		public CompletedProzessState(int result) {
			this.result = result;
		}
		@Override public <X> X visit(ProzessHandler<X> handler) { return handler.acceptResult(this.result); }
	}
	public interface ProzessHandler<X> {
		public X accept(ProtoProzessState prozess);
		public X accept(RunnableProzessState prozess);
		public X accept(AsyncProzessState prozess);
		public X acceptResult(int exitCode);
	}
	
	public <X> X visit(ProzessHandler<X> handler);
}
