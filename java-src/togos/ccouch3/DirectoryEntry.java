package togos.ccouch3;

public class DirectoryEntry extends FileInfo implements Comparable<DirectoryEntry>
{
	public final String name;
	
	public DirectoryEntry( String name, String urn, FileType fileType, long size, long mtime ) {
		super( name, urn, fileType, size, mtime );
		this.name = name;
	}
		
	public DirectoryEntry( String name, FileInfo fileInfo ) {
		super(fileInfo);
		this.name = name;
	}
	
	@Override
	public int compareTo(DirectoryEntry o) {
		return name.compareTo(o.name);
	}
}
