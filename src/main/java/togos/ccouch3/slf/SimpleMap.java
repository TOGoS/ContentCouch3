package togos.ccouch3.slf;

public interface SimpleMap<K,V>
{
	public void put( K k, V v );
	public V get( K k );
}
