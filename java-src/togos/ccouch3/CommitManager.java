package togos.ccouch3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import togos.blob.ByteChunk;
import togos.blob.SimpleByteChunk;
import togos.blob.util.BlobUtil;
import togos.ccouch3.FlowUploader.CommitConfig;
import togos.ccouch3.hash.StreamURNifier;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.StoreException;
import togos.ccouch3.util.FileUtil;

public class CommitManager
{
	protected static final String[] EMPTY_STRING_ARRAY = new String[0];
	public final Repository repo;
	protected final StreamURNifier digestor;
	
	public CommitManager( Repository repo, StreamURNifier digestor ) {
		this.repo = repo;
		this.digestor = digestor;
	}
	
	protected static File commitListFile( File target ) {
		return new File( target + (target.isDirectory() ? "/" : "") + ".commit-uris" );
	}
	
	protected static String[] getOldCommitUrns( File target ) throws IOException {
		if( !target.exists() ) return EMPTY_STRING_ARRAY;
		File commitListFile = commitListFile( target );
		if( !commitListFile.exists() ) return EMPTY_STRING_ARRAY;
		ArrayList<String> urns = new ArrayList<String>();
		BufferedReader r = new BufferedReader(new FileReader( commitListFile ));
		try {
			for( String l; (l = r.readLine()) != null; ) {
				l = l.trim();
				if( l.length() == 0 || l.startsWith("#") ) continue;
				urns.add(l);
			}
		} finally {
			r.close();
		}
		return urns.toArray(new String[urns.size()]);
	}
	
	protected void putCommitUrn( File target, String commitUrn ) throws IOException {
		File commitListFile = commitListFile( target );
		FileUtil.mkParentDirs( commitListFile );
		FileWriter w = new FileWriter( commitListFile );
		w.write( commitUrn + "\n" );
		w.close();
	}
	
	/**
	 * Should remove redundant commit URNs (URNs of commits that are
	 * ancestors of other commits in the list) from the given list.
	 */
	protected String[] cleanCommitList( String[] commitUrns ) {
		// This is currently not implemented.
		return commitUrns;
	}
	
	protected static String stripSubjectPrefix( String urn ) {
		if( urn.startsWith("x-rdf-subject:") ) return urn.substring(14);
		if( urn.startsWith("x-parse-rdf:") ) return urn.substring(12);
		return urn;
	}
	
	protected Commit getBasicCommitInfo( ByteChunk chunk ) {
		if( chunk == null ) return null;
		
		return RDFCommitExtractor.getBasicXmlCommitInfo( BlobUtil.string( chunk ) );
	}
	
	public class CommitSaveResult {
		public final String latestCommitUrn;
		public final String latestCommitDataUrn;
		public final ByteChunk latestCommitData;
		public final boolean newCommitCreated;
		public CommitSaveResult( String commitUrn, String commitDataUrn, ByteChunk commitData, boolean newCommit ) {
			this.latestCommitUrn = commitUrn;
			this.latestCommitDataUrn = commitDataUrn;
			this.latestCommitData = commitData;
			this.newCommitCreated = newCommit;
		}
	}
	
	/**
	 * Save a new commit if there is anything to save, 
	 * @param targetPath
	 * @param targetUrn
	 * @param cc
	 */
	protected CommitSaveResult saveCommit( File target, String targetUrn, long timestamp, CommitConfig cc )
		throws IOException, StoreException
	{
		assert targetUrn != null;
		
		String[] oldCommitUrns = getOldCommitUrns( target );
		for( String oldCommitUrn : oldCommitUrns ) {
			String blobUrn = stripSubjectPrefix(oldCommitUrn);
			ByteChunk data = repo.getChunk( blobUrn, 4096 );
			Commit c = getBasicCommitInfo( data );
			if( c != null && targetUrn.equals(c.targetUrn) ) {
				return new CommitSaveResult( oldCommitUrn, blobUrn, data, false );
			}
		}
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		RDFCommitSerializer.serializeCommit(cc.toCommit( targetUrn, oldCommitUrns, timestamp), baos );
		byte[] commitData = baos.toByteArray();
		String blobUrn = digestor.digest( new ByteArrayInputStream(commitData) );
		repo.put( blobUrn, new ByteArrayInputStream( commitData ) );
		String commitUrn = "x-rdf-subject:" + blobUrn;
		putCommitUrn( target, commitUrn );
		
		return new CommitSaveResult( commitUrn, blobUrn, new SimpleByteChunk(commitData), true );
	}
}
