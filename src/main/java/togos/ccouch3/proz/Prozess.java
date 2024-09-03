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
public interface Prozess<C,R> {
	public interface RunnableProzess<C,R> {
		public abstract Prozess<C,R> run();
	}
	public interface AsyncProzess<C,R> {
		public abstract void abort();
		public abstract Prozess<C,R> waitFor() throws InterruptedException;
	}
	public interface ProtoProzess<C,R> {
		public Prozess<C,R> start(C ctx);
	}
	// Functional versions also possible; add when necessary
	
	/**
	 * @param <P> prozess type
	 * @param <R> prozess result type
	 * @param <X> handler result type
	 */
	public interface ProzessHandler<C,R,X> {
		public X accept(ProtoProzess<C,R> prozess);
		public X accept(RunnableProzess<C,R> prozess);
		public X accept(AsyncProzess<C,R> prozess);
		public X acceptResult(R result);
	}
	
	public <X> X visit(ProzessHandler<C,R,X> handler);
}
