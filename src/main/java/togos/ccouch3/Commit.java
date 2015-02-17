package togos.ccouch3;

public class Commit
{
	public static final long TIMESTAMP_NONE = -1;
	
	public final String targetUrn; 
	public final String[] parentCommitUrns;
	public final String[] tags;
	public final String authorName;
	public final String description;
	/** Millisecond-based timestamp */
	public final long timestamp;
	
	public Commit( String targetUrn, String[] parentCommitUrns, String[] tags, String authorName, String description, long timestamp ) {
		assert parentCommitUrns != null;
		assert tags != null;
		this.targetUrn        = targetUrn;
		this.parentCommitUrns = parentCommitUrns;
		this.tags             = tags;
		this.authorName       = authorName;
		this.description      = description;
		this.timestamp        = timestamp;
	}
}
