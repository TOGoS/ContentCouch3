package togos.ccouch3;

import java.io.File;

//TODO: I don't think it makes that much sense for a
//directory entry to *be* the file it points to.
//Maybe change that.
public class DirectoryEntry extends FileInfo implements Comparable<File>
{
	private static final long serialVersionUID = 1L;
	
	public final String name;
	
	public DirectoryEntry( String name, String urn, FSObjectType fileType, long size, long mtime ) {
		super( name, urn, fileType, size, mtime );
		this.name = name;
	}
	
	public DirectoryEntry( String name, FileInfo fileInfo ) {
		super(fileInfo);
		this.name = name;
	}
	
	@Override public int compareTo(File o) {
		if( o instanceof DirectoryEntry ) {
			return name.compareTo( ((DirectoryEntry)o).name );
		} else {
			return super.compareTo(o);
		}
	}
}
