package peregrine.util;

import java.util.*;
import java.util.concurrent.*;

public class MarkMap<T,V> {

	protected ConcurrentHashMap<T,V> map = new ConcurrentHashMap();

    protected List<MarkListener<T>> listeners = new ArrayList();
    
	public void mark( T key ) {
        put( key, null );
	}

    protected void put( T key, V value ) {

        boolean updated = map.get( key ) == null;
        
		map.put( key, value );

        if ( updated ) 
            fireUpdated( key, MarkListener.Status.MARKED );
        
    }

    public V get( T key ) {
        return map.get( key );
    }

	public void clear( T key ) {

        boolean updated = map.get( key ) != null;

        map.remove( key );

        if ( updated ) 
            fireUpdated( key, MarkListener.Status.CLEARED );
        
	}

	public boolean contains( T key ) {
		return map.containsKey( key );
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

        for( T key : map.keySet() ) {
            listener.updated( key, MarkListener.Status.MARKED );
        }
        
    }
    
    protected void fireUpdated( T key, MarkListener.Status status ) {

        for( MarkListener current : listeners ) {
            current.updated( key, status );
        }
        
    }

}