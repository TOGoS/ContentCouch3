package togos.ccouch3.util;

public interface AddableSet<T>
{
	public boolean contains( T val );
	public void add( T val );
}