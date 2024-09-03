package togos.ccouch3.proz;

import togos.ccouch3.proz.ProzessState.AsyncProzessState;
import togos.ccouch3.proz.ProzessState.ProtoProzessState;
import togos.ccouch3.proz.ProzessState.ProzessHandler;
import togos.ccouch3.proz.ProzessState.RunnableProzessState;

public class ProzessRunner implements ProzessHandler<ProzessState> {
	final ProzessContext ctx;
	ProzessState state;
	int exitCode = -1;
	public ProzessRunner(ProzessState state, ProzessContext ctx) {
		this.state = state;
		this.ctx = ctx;
	}
	public int run() {
		while( state != null ) state = state.visit(this);
		return exitCode;
	}
	@Override public ProzessState accept(AsyncProzessState prozess) {
		try {
			return prozess.waitFor();
		} catch (InterruptedException e) {
			prozess.abort();
			Thread.currentThread().interrupt();
			return null;
		}
	}
	@Override public ProzessState accept(ProtoProzessState prozess) {
		return prozess.start(ctx);
	}
	@Override public ProzessState accept(RunnableProzessState prozess) {
		return prozess.run();
	}
	@Override public ProzessState acceptResult(int exitCode) {
		this.exitCode = exitCode;
		return null;
	}
	
	public static int run(ProzessState s, ProzessContext ctx) {
		return new ProzessRunner(s, ctx).run();
	}
}