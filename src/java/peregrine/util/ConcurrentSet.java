package peregrine.util;

import java.util.*;
import java.util.concurrent.*;

public class ConcurrentSet<T> {

	protected ConcurrentHashMap<T,Long> map = new ConcurrentHashMap();
	
	public void mark( T entry ) {
		map.put( entry, 1L );
	}
		
	public void clear( T entry ) {
		map.remove( entry );
	}

	public boolean contains( T entry ) {
		return map.get( entry ) != null;
	}

    public int size() {
       return map.size();
    }

    public String toString() {
    	return map.keySet().toString();
	}

    public Collection<T> values() {
        return map.keySet();
    }    
	
}
