package togos.ccouch3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;

import org.bitpedia.util.Base32;

import togos.ccouch3.util.FileUtil;

public class NormalLogger implements Logger
{
	protected final File logDir;
	public NormalLogger( File logDir ) {
		this.logDir = logDir;
	}
	
	public String dumpToLog( byte[] data ) throws IOException {
		MessageDigest digestor;
		try {
			digestor = MessageDigest.getInstance("SHA-1");
		} catch( NoSuchAlgorithmException e ) {
			throw new RuntimeException( "sha1-not-found-which-is-ridiculous", e );
		}
		digestor.update(data);
		String b32 = Base32.encode(digestor.digest());
		
		
		Calendar cal = Calendar.getInstance();
		File logFile = new File(logDir,
			cal.get(Calendar.YEAR) + File.separator +
			cal.get(Calendar.MONTH+1) + File.separator +
			cal.get(Calendar.DAY_OF_MONTH) + File.separator +
			b32+".log");
		
		System.err.println("Attempting to dump some crap into "+logFile+"...");
		FileUtil.mkParentDirs(logFile);
		FileOutputStream fos = new FileOutputStream(logFile); 
		try {
			fos.write(data);
		} finally {
			fos.close();
		}
		
		return logFile.getPath();
	}
}
