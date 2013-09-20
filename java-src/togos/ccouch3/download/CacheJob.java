package togos.ccouch3.download;


class CacheJob {
	final RecurseOptions recurseOptions;
	final String urn;
	
	public CacheJob( String urn ) {
		this.recurseOptions = new RecurseOptions();
		this.urn = urn;
	}
}
