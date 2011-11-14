package peregrine.util;

import java.util.*;
import java.util.concurrent.*;

public class MarkMap<T,V> {

	protected ConcurrentHashMap<T,V> map = new ConcurrentHashMap();

    protected List<MarkListener<T>> listeners = new ArrayList();
    
	public void mark( T entry ) {
		map.put( entry, null );
        fireMarked( entry );
	}
		
	public void clear( T entry ) {
		map.remove( entry );
        fireCleared( entry );
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

    public void addListener( MarkListener<T> listener ) {
        this.listeners.add( listener );
    }

    /**
     * Add a listener and then call marked() on the current marks.
     */
    public void addListenerWithMarks( MarkListener<T> listener ) {

        addListener( listener );

        for( T entry : map.keySet() ) {
            listener.marked( entry );
        }
        
    }
    
    protected void fireMarked( T entry ) {

        for( MarkListener current : listeners ) {
            current.marked( entry );
        }
        
    }

    protected void fireCleared( T entry ) {

        for( MarkListener current : listeners ) {
            current.cleared( entry );
        }
        
    }

}