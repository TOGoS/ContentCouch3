package togos.ccouch3.proz;

import togos.ccouch3.proz.Prozess.AsyncProzess;
import togos.ccouch3.proz.Prozess.ProtoProzess;
import togos.ccouch3.proz.Prozess.ProzessHandler;
import togos.ccouch3.proz.Prozess.RunnableProzess;

public class ProzessRunner<C,R> implements ProzessHandler<C,R,Prozess<C,R>> {
	final C ctx;
	Prozess<C,R> state;
	R result = null;
	public ProzessRunner(Prozess<C,R> state, C ctx) {
		this.state = state;
		this.ctx = ctx;
	}
	public R run() {
		while( state != null ) state = state.visit(this);
		return result;
	}
	@Override public Prozess<C,R> accept(AsyncProzess<C,R> prozess) {
		try {
			return prozess.waitFor();
		} catch (InterruptedException e) {
			prozess.abort();
			Thread.currentThread().interrupt();
			return null;
		}
	}
	@Override public Prozess<C,R> accept(ProtoProzess<C,R> prozess) {
		return prozess.start(ctx);
	}
	@Override public Prozess<C,R> accept(RunnableProzess<C,R> prozess) {
		return prozess.run();
	}
	@Override public Prozess<C,R> acceptResult(R result) {
		this.result = result;
		return null;
	}
	
	public static <C,R> R run(Prozess<C,R> s, C ctx) {
		return new ProzessRunner<C,R>(s, ctx).run();
	}
}
