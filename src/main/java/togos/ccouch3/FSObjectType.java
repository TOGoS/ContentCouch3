package togos.ccouch3;

public enum FSObjectType {
	BLOB("Blob"),
	DIRECTORY("Directory"),
	COMMIT("Commit"),
	UNKNOWN("Unknown");

	public final String niceName;
	FSObjectType( String niceName ) {
		this.niceName = niceName;
	}
}
