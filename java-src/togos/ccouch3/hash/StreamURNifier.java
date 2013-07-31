package togos.ccouch3.hash;

import java.io.IOException;
import java.io.InputStream;

public interface StreamURNifier {
	public String digest( InputStream is ) throws IOException;
}
