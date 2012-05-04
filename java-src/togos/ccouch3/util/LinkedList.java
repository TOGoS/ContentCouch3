package togos.ccouch3.util;

public class LinkedList<E> {
	@SuppressWarnings("rawtypes")
	public static final LinkedList EMPTY = new LinkedList(); 
	
	public final int length;
	public final E headValue;
	public final LinkedList<E> rest;
	
	protected LinkedList() {
		this.length = 0;
		this.headValue = null;
		this.rest = this;
	}
	
	public LinkedList( int length, E head, LinkedList<E> rest ) {
		this.length = length;
		this.headValue = head;
		this.rest = rest;
	}
	
	public LinkedList( E headValue, LinkedList<E> rest) {
		this( rest.length + 1, headValue, rest );
	}

	@SuppressWarnings("unchecked")
	public static <E> LinkedList<E> from( E[] arr, int begin, int length ) {
		if( length == 0 ) return EMPTY;
		return from(arr,begin+1,length-1).prepend(arr[begin]);
	}
	
	public static <E> LinkedList<E> from( E[] arr ) {
		return from( arr, 0, arr.length );
	}
	
	@SuppressWarnings("unchecked")
	public static <E> LinkedList<E> empty() {
		return EMPTY;
	}
	
	public LinkedList<E> reverse() {
		LinkedList<E> arrest = this;
		@SuppressWarnings("unchecked")
		LinkedList<E> previous = EMPTY;
		while( arrest.length > 0 ) {
			previous = previous.prepend( arrest.headValue );
			arrest = arrest.rest;
		}
		return previous;
	}
	
	public LinkedList<E> prepend( E value ) {
		return new LinkedList<E>( value, this );
	}
}
