package togos.ccouch3.proz;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public interface ProzessContext {
	public File getPwd();
	public Map<String,String> getEnv();
	public OutputStream getOutputStream(int fd);
	public InputStream getInputStream(int fd);
	// Methods to read/write resources go here
}