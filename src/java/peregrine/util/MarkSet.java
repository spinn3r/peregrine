package peregrine.util;

public class MarkSet<T> extends MarkMap<T,Long>{

	@Override
	public void mark( T entry ) {
		map.put( entry, 1L );
	}
	
}
