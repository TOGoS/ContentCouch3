package togos.ccouch3;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import togos.ccouch3.repo.Repository;
import togos.ccouch3.util.Action;
import togos.ccouch3.util.Charsets;
import togos.ccouch3.util.Consumer;
import togos.ccouch3.util.ListUtil;
import togos.ccouch3.util.ParseResult;
import togos.ccouch3.util.UsageError;

class FoundItem {
	public final String urn;
	public final File file;
	public FoundItem(String urn, File file) {
		this.urn = urn;
		this.file = file;
	}
	public String toString() {
		return "FoundItem["+urn+", "+file+"]";
	}
}

// TODO: Import TScript34-P0010 and use its function interface
interface Function<I,O> {
	O apply(I input);
}

class Constant<I,O> implements Function<I,O> {
	final O value;
	public Constant(O value) { this.value = value; }
	@Override public O apply(I input) { return value; }
}

class FoundItemURN implements Function<FoundItem,byte[]> {
	private FoundItemURN() { }
	public static final FoundItemURN instance = new FoundItemURN();
	@Override public byte[] apply(FoundItem input) {
		return input.urn.getBytes(Charsets.UTF8);
	}
}
class FoundItemFile implements Function<FoundItem,File> {
	private FoundItemFile() {}
	public static FoundItemFile instance = new FoundItemFile();
	@Override public File apply(FoundItem input) {
		return input.file;
	}
}
class FileURI implements Function<File,byte[]> {
	private FileURI() {}
	public static FileURI instance = new FileURI();
	@Override public byte[] apply(File input) {
		return ("file:"+input.getPath().replace("%", "%37").replace(" ","%20")).getBytes(Charsets.UTF8);
	}
}
class FilePath implements Function<File,byte[]> {
	private FilePath() {}
	public static FilePath instance = new FilePath();
	@Override public byte[] apply(File input) {
		return input.getPath().getBytes(Charsets.UTF8);
	}
}

class FunctionUtil {
	private static class Composed<T0,T1,T2> implements Function<T0,T2> {
		final Function<T0,T1> f0;
		final Function<T1,T2> f1;
		public Composed(Function<T0,T1> f0, Function<T1,T2> f1) {
			this.f0 = f0;
			this.f1 = f1;
		}
		@Override public T2 apply(T0 input) {
			return f1.apply(f0.apply(input));
		}
	}
	
	public static <T0,T1,T2> Function<T0,T2> compose(Function<T0,T1> f0, Function<T1,T2> f1) {
		return new Composed<T0,T1,T2>(f0, f1);
	}
}

class Formatter<I> implements Function<I,byte[]> {
	final Function<I,byte[]>[] components;
	@SafeVarargs
	Formatter(Function<I,byte[]>...components) {
		this.components = components;
	}
	
	@SuppressWarnings("unchecked")
	static <I> Formatter<I> concat(List<Function<I,byte[]>> components) {
		return new Formatter<I>(components.toArray(new Function[components.size()]));
	}
	
	public static final byte[] concat( byte[]...arrs ) {
		int len = 0;
		for( byte[] arr : arrs ) len += arr.length;
		byte[] result = new byte[len];
		int i=0;
		for( byte[] arr : arrs ) {
			for( int j=0; j<arr.length; ++j ) result[i++] = arr[j];
		}
		return result;
	}
	
	public byte[] apply(I fi) {
		byte[][] comps = new byte[components.length][];
		for( int i=0; i<comps.length; ++i ) comps[i] = components[i].apply(fi);
		return concat(comps);
	}
}

class Dumper<T,R> implements Action<Consumer<T>,R> {
	final T item;
	final R r;
	public Dumper(T item, R r) {
		this.item = item;
		this.r = r;
	}
	
	@Override
	public R execute(Consumer<T> dest) {
		dest.accept(this.item);
		return this.r;
	}
}

public class FindFilesCommand
implements Action<Consumer<FoundItem>,Integer> {
	final Repository[] repos;
	final String[] urns;
	
	public FindFilesCommand(
		Repository[] repos,
		String[] urns
	) {
		this.repos = repos;
		this.urns = urns;
	}
	
	@Override public Integer execute(Consumer<FoundItem> dest) {
		for( Repository repo : repos ) {
			for( String urn : urns ) {
				try {
					for( File f : repo.getFiles(urn) ) {
						dest.accept(new FoundItem(urn, f));
					}
				} catch( Exception e ) {
					System.err.println(e.getMessage());
				}
			}
		}
		return Integer.valueOf(0);
	}
	
	static final Pattern FORMAT_TOKEN_REGEX = Pattern.compile("([^\\{]+)|\\{([^\\}]+)\\}");
	static Function<FoundItem,byte[]> parseFormat(String formatString)
	throws ParseException
	{
		Matcher m = FORMAT_TOKEN_REGEX.matcher(formatString);
		List<Function<FoundItem,byte[]>> components = new ArrayList<Function<FoundItem,byte[]>>();
		int index = 0;
		while( m.find(index) ) {
			if( m.group(1) != null ) {
				components.add(new Constant<FoundItem,byte[]>(m.group(1).getBytes(Charsets.UTF8)));
			} else if( m.group(2) != null ) {
				String name = m.group(2);
				if( "0".equals(name) ) {
					components.add(new Constant<FoundItem,byte[]>(new byte[] {0}));
				} else if( "lf".equals(name) ) {
					components.add(new Constant<FoundItem,byte[]>(("\n").getBytes(Charsets.UTF8)));
				} else if( "filepath".equals(name) ) {
					components.add(FunctionUtil.compose(FoundItemFile.instance, FilePath.instance));
				} else if( "urn".equals(name) ) {
					components.add(FoundItemURN.instance);
				} else {
					throw new ParseException("Unrecognized identifier in format: '"+name+"'", index);
				}
			}
			index = m.end();
		}
		return Formatter.concat(components);
	}
	
	static final Pattern FORMAT_ARG_PATTERN = Pattern.compile("^--format=(.*)$");
	
	static final String HELP_TEXT =
		"Usage: find-files <options> <urn> ...\n"+
		"\n"+
		"Find and list files in local repositories corresponding\n"+
		"to the named resources.\n"+
		"\n"+
		"Options:\n" +
		"  --help                   ; show this help text and exit\n" +
		"  --format=<format string> ; output a specific string for each found item;\n" +
		"  -print0                  ; use NUL instead of LF as separator\n" +
		"                           ; for otherwise-default format\n" +
		"\n" +
		"Example:\n" +
		"  find-files --format=\"the file for {urn} is {filepath}{lf}\" urn:sha1:FOOBAR\n" +
		"\n" +
		"Format is a sequence of:\n"+
		"  \"literal value\"\n" +
		"  \"{0}\"  ; a NUL byte\n"+
		"  \"{lf}\" ; a newline character\n"+
		"\n"+
		"No record separator is implied in format strings.\n"+
		"Default format is effectively \"{filepath}{lf}\"\n";
	
	protected static Action<Consumer<byte[]>,Integer> parse(CCouchContext ctx, List<String> args)
	throws UsageError
	{
		List<String> urns = new ArrayList<String>();
		Function<FoundItem,byte[]> formatter = null;
		byte[] separator = "\n".getBytes(Charsets.UTF8);
		Matcher m;
		while( !args.isEmpty() ) {
			ParseResult<List<String>,CCouchContext> ctxPr = ctx.handleCommandLineOption(args);
			if( ctxPr.remainingInput != args ) {
				args = ctxPr.remainingInput;
				ctx  = ctxPr.result;
				continue;
			}
			
			String arg = ListUtil.car(args);
			args = ListUtil.cdr(args);
			if( !arg.startsWith("-") ) {
				urns.add(arg);
			} else if( (m = FORMAT_ARG_PATTERN.matcher(arg)).matches() ) {
				try {
					formatter = parseFormat(m.group(1));
				} catch (ParseException e) {
					throw new UsageError(e.getMessage()+" at index "+e.getErrorOffset());
				}
			} else if( CCouch3Command.isHelpArgument(arg) ) {
				return new Dumper<byte[],Integer>(HELP_TEXT.getBytes(Charsets.UTF8), Integer.valueOf(0));
			} else if( "-print0".equals(arg) ) {
				separator = new byte[] { 0 };
			} else {
				throw new UsageError("Unrecognized argument: '"+arg+"'");
			}
		}
		
		if( formatter == null ) {
			formatter = new Formatter<FoundItem>(
				FunctionUtil.compose(
					FoundItemFile.instance,
					FilePath.instance
				), new Constant<FoundItem,byte[]>(separator)
			);
		}
		
		final Function<FoundItem,byte[]> _formatter = formatter;
		
		ctx = ctx.fixed(); // Ow hacky!
		final FindFilesCommand ffc = new FindFilesCommand(ctx.getLocalRepositories(), urns.toArray(new String[urns.size()]));
		return new Action<Consumer<byte[]>,Integer>() {
			@Override
			public Integer execute(final Consumer<byte[]> dest) {
				return ffc.execute(new Consumer<FoundItem>() {
					@Override public void accept(FoundItem value) {
						dest.accept(_formatter.apply(value));
					}
				});
			}
		};
	}
	
	public static int main(CCouchContext ctx, List<String> args) throws IOException, InterruptedException {
		Action<Consumer<byte[]>,Integer> cmd;
		try {
			cmd = parse(ctx, args);
		} catch( UsageError e ) {
			System.err.println(e.getMessage());
			return 1;
		}
		return CCouch3Command.run(cmd, System.out);
	}
}
