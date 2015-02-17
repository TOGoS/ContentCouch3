package togos.ccouch3.util;

public class EmptyAddableSet<T> implements AddableSet<T> 
{
	private static EmptyAddableSet<Object> instance = new EmptyAddableSet<Object>();
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> EmptyAddableSet<T> getInstance() {
		return (EmptyAddableSet<T>)(EmptyAddableSet)instance;
	}
	
	private EmptyAddableSet() { }
	
	@Override public boolean contains(T val) { return false; }
	@Override public void add(T val) {}
}
