package peregrine.util;

import java.util.*;
import java.util.concurrent.*;

public class MarkMap<T,V> {

	protected ConcurrentHashMap<T,V> map = new ConcurrentHashMap();

    protected List<MarkListener<T>> listeners = new ArrayList();
    
	public void mark( T entry ) {
		map.put( entry, null );
        fireUpdated( entry, MarkListener.Status.MARKED );
	}
		
	public void clear( T entry ) {
		map.remove( entry );
        fireUpdated( entry, MarkListener.Status.CLEARED );
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
     * Add a listener and then get a snapshot of the current state.  This can be
     * used to sync the behavior of two objects and avoid any races. It IS
     * possible that the updated method is called 2x on the same object but this
     * is acceptable for our usage.
     */
    public void addListenerWithSnapshot( MarkListener<T> listener ) {

        addListener( listener );

        for( T entry : map.keySet() ) {
            listener.updated( entry, MarkListener.Status.MARKED );
        }
        
    }
    
    protected void fireUpdated( T entry, MarkListener.Status status ) {

        for( MarkListener current : listeners ) {
            current.updated( entry, status );
        }
        
    }

}