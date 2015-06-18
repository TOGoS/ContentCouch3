package togos.ccouch3.rdf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MultiMap<K,V> extends HashMap<K,Set<V>>
{
	private static final long serialVersionUID = 1L;

	public MultiMap() {
		super();
	}

	public MultiMap(MultiMap<K,V> n) {
		for( Iterator<Map.Entry<K,Set<V>>> i=n.entrySet().iterator(); i.hasNext(); ) {
			Map.Entry<K,Set<V>> e = i.next();
			put( e.getKey(), new HashSet<V>(e.getValue()) );
		}
	}
	
	public void add(K key, V value) {
		Set<V> i = get(key);
		if( i == null ) {
			put(key, i = new HashSet<V>());
		}
		i.add(value);
	}
	
	public void putSingle(K key, V value) {
		Set<V> i = new HashSet<V>();
		i.add(value);
		put(key, i);
	}

	public Set<V> getSet(K key) {
		return get(key);
	}
	
	public V getSingle(K key, V defaultValue) {
		Set<V> i = get(key);
		if( i == null ) return defaultValue;
		for( Iterator<V> ii=i.iterator(); ii.hasNext(); ) return ii.next();
		return defaultValue;
	}

	public void importValues(Map<K,V> properties) {
		for( Iterator<Map.Entry<K,V>> i = properties.entrySet().iterator(); i.hasNext(); ) {
			Map.Entry<K,V> entry = (Map.Entry<K,V>)i.next();
			add(entry.getKey(), entry.getValue());
		}
	}
}