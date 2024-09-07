package togos.ccouch3.rdf;

import java.util.Collections;
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
	
	public void importValues(Map<K,V> properties) {
		for( Iterator<Map.Entry<K,V>> i = properties.entrySet().iterator(); i.hasNext(); ) {
			Map.Entry<K,V> entry = (Map.Entry<K,V>)i.next();
			add(entry.getKey(), entry.getValue());
		}
	}
	
	//// Static methods for working with immutable maps
	
	public static <K,V> Set<V> getSet(Map<K,Set<V>> mmap, K key) {
		Set<V> set = mmap.get(key);
		return set == null ? Collections.<V>emptySet() : set;
	}
	
	public static <K,V> V getSingle(Map<K,Set<V>> mmap, K key, V defaultValue) {
		Set<V> i = mmap.get(key);
		if( i == null ) return defaultValue;
		for( Iterator<V> ii=i.iterator(); ii.hasNext(); ) return ii.next();
		return defaultValue;
	}
	
	static <V> Set<V> withValue(Set<V> original, V v) {
		if( original.isEmpty() ) return Collections.singleton(v);
		Set<V> updated = new HashSet<V>(original);
		updated.add(v);
		return Collections.unmodifiableSet(updated);
	}
	
	public static <K,V> Map<K,Set<V>> withValue(Map<K,Set<V>> mmap, K k, V v) {
		if( getSet(mmap, k).contains(v) ) return mmap;
		
		Map<K,Set<V>> updated = new HashMap<K,Set<V>>(mmap);
		updated.put(k, withValue(mmap.get(k), v));
		return Collections.unmodifiableMap(updated);
	}
	
	public static <K,V> Map<K,Set<V>> of(K k, V v) {
		return Collections.singletonMap(k, Collections.singleton(v));
	}
	
	static <V> Set<V> freeze(Set<V> original) {
		if( original.size() == 0 ) return Collections.emptySet();
		return Collections.unmodifiableSet(original);
	}
	
	public static <K,V> Map<K,Set<V>> freeze(Map<K,Set<V>> original) {
		Map<K,Set<V>> frozen = new HashMap<K,Set<V>>();
		boolean anyChanges = false;
		for( Map.Entry<K,Set<V>> e : original.entrySet() ) {
			Set<V> values = e.getValue();
			Set<V> newValues = freeze(values);
			if( values.size() == 0 ) {
				anyChanges = true;
				continue;
			}
			frozen.put(e.getKey(), newValues);
			if( newValues != values) anyChanges = true;
		}
		return frozen.isEmpty() ? Collections.<K,Set<V>>emptyMap() :
			anyChanges ? Collections.unmodifiableMap(frozen) :
			Collections.unmodifiableMap(original);
	}
}
