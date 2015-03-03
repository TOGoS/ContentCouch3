package togos.ccouch3;

public enum FSObjectType {
	BLOB("Blob"),
	DIRECTORY("Directory");

	public final String niceName;
	FSObjectType( String niceName ) {
		this.niceName = niceName;
	}
}
