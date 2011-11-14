package peregrine.util;

public class MarkSet<T> extends MarkMap<T,Long>{

    private static final Long MARKED = new Long( 1L );
    
	@Override
	public void mark( T entry ) {
		put( entry, MARKED );
	}

}
