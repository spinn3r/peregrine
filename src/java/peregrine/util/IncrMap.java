package peregrine.util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Simpilar to MarkMap but we keep track of and incrementing counter per key.
 */
public class IncrMap<T> {

    Map<T,AtomicInteger> map = new ConcurrentHashMap();

    public IncrMap( Set<T> list ) {

        for( T key : list ) {
            map.put( key, new AtomicInteger() );
        }
        
    }
    
    public void incr( T key ) {
        map.get( key ).getAndIncrement();
    }

    public void decr( T key ) {
        map.get( key ).getAndDecrement();
    }

    public int get( T key ) {
        return map.get( key ).get();
    }

}
