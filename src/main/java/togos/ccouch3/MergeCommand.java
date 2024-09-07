package togos.ccouch3;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import togos.ccouch3.proz.Prozess;
import togos.ccouch3.proz.ProzessRunner;
import togos.ccouch3.proz.Prozesses;
import togos.ccouch3.proz.Prozesses.RunnableWithSystemContextProzessImpl;
import togos.ccouch3.proz.SystemContext;
import togos.ccouch3.util.StreamUtil;
import togos.ccouch3.util.UsageError;

public class MergeCommand extends RunnableWithSystemContextProzessImpl {
	final CCouchContext ccCtx;
	final List<String> refs;
	
	public MergeCommand(CCouchContext ccctx, List<String> refs) {
		this.ccCtx = ccctx;
		this.refs  = refs ;
	}
	
	static class URIRef implements Comparable<URIRef> {
		public final String uri;
		public URIRef(String uri) {
			this.uri = uri;
		}
		@Override public boolean equals(Object obj) {
			if( !(obj instanceof URIRef) ) return false;
			return uri.equals(((URIRef)obj).uri);
		}
		@Override public int hashCode() {
			return 381 + uri.hashCode();
		}
		@Override public int compareTo(URIRef o) {
			return uri.compareTo(o.uri);
		}
	}
	
	static class MergeObject<T> {
		enum Type {
			Directory,
			Commit,
			Blob,
			ArbitraryRDF
		}
		URIRef parsedFromRef;
		String parsedFrom;
		URIRef ref;
		URIRef className;
		T payload;
	}
	
	static interface Converter<I,O> {
		public <T extends O> T convert(Object I, Class<T> expectedClass) throws IOException;
	}
	static interface Storer<I,O> {
		public O store(I item) throws IOException;
	}
	
	static interface MergeContext extends Converter<Object,Object>, Storer<Object,URIRef> {}
	
	static URIRef merge( Directory dir1, Directory dir2, MergeContext ctx ) {
		throw new RuntimeException("todo");
	}
	
	static URIRef merge( Object o1, Object o2, MergeContext ctx ) { 
		throw new RuntimeException("todo"); 
	}
	
	@Override public Prozess<SystemContext,Integer> run(SystemContext ctx) {
		PrintStream errout = StreamUtil.toPrintStream(ctx.getOutputStream(2));
		String curref = refs.get(0);
		for( int i=1; i<refs.size(); ++i ) {
			errout.println("Merge: Not yet implemented!");
			errout.println("But if it were, you'd be merging "+refs.get(i)+" onto "+curref);
			return Prozesses.done(1);
		}
		StreamUtil.toPrintStream(ctx.getOutputStream(1)).println(curref);
		return Prozesses.done(0);
	}
	
	public static MergeCommand parse(CCouchContext ccCtx, List<String> args) throws UsageError {
		List<String> refs = new ArrayList<String>();
		for( String arg : args ) {
			if( !arg.startsWith("-") ) {
				refs.add(arg);
			} else {
				throw new UsageError("Invalid argument to `merge`: "+arg);
			}
		}
		if( refs.isEmpty() ) {
			throw new UsageError("Merge requires at least one ref");
		}
		return new MergeCommand(ccCtx, refs);
	}
	
	public static int main(CCouchContext ccctx, List<String> args, SystemContext ctx) {
		MergeCommand cmd;
		try {
			cmd = parse(ccctx, args);
		} catch( UsageError err ) {
			StreamUtil.toPrintStream(ctx.getOutputStream(2)).println(err.getMessage());
			return 1;
		}
		return ProzessRunner.run(cmd, ctx);
	}
}
