package togos.ccouch3.proz;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class SystemContextImpl
implements SystemContext
{
	final File pwd;
	final Map<String,String> env;
	Object[] io;
	public SystemContextImpl(File pwd, Map<String,String> env, Object[] io) {
		this.pwd = pwd;
		this.env = env;
		this.io = io;
	}
	
	@Override public Map<String, String> getEnv() { return env; }
	
	@Override public File getPwd() { return pwd; }
	
	static class NullInputStream extends InputStream {
		static final NullInputStream INSTANCE = new NullInputStream();
		private NullInputStream() {}
		public int read()      { return -1; }
		public int available() { return 0; }
	}
	
	static class NullOutputStream extends OutputStream {
		static final NullOutputStream INSTANCE = new NullOutputStream();
		private NullOutputStream() {}
		// Unlike PRocessBuilder's NullOutputStream,
		// this one acts more like /dev/null and just ignores the data.
		public void write(int b) {}
		public void write(byte[] data, int offset, int length) {}
	}
	
	@Override public InputStream getInputStream(int fd) {
		if( fd < 0 || fd >= io.length || !(io[fd] instanceof InputStream) ) return NullInputStream.INSTANCE;
		return (InputStream)io[fd];
	}
	@Override public OutputStream getOutputStream(int fd) {
		if( fd < 0 || fd >= io.length || !(io[fd] instanceof OutputStream) ) return NullOutputStream.INSTANCE;
		return (OutputStream)io[fd];
	}
}
