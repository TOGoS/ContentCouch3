package togos.ccouch3.hash;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;


public class MessageDigestStreamURNifier implements StreamURNifier {
	final MessageDigestFactory messageDigestFactory;
	final HashFormatter hform;
	
	public MessageDigestStreamURNifier( MessageDigestFactory fac, HashFormatter form ) {
		this.messageDigestFactory = fac;
		this.hform = form;
	}
	
	@Override
	public String digest(InputStream is) throws IOException {
		MessageDigest d = messageDigestFactory.createMessageDigest();
		
		byte[] buffer = new byte[65536];
		while( true ) {
			int z = is.read( buffer );
			if( z <= 0 ) break;
			d.update( buffer, 0, z );
				
		}
		
		return hform.format( d.digest() );
	}
}