
package peregrine.util;

import java.util.*;
import java.util.concurrent.*;

/**
 * Uninterruptable version of LinkedBlockingQueue.  Code should not be calling
 * interrrupt() on us and hence these InterrruptExceptions are pointless.
 */
public class SimpleBlockingQueue<T> {

    BlockingQueue<T> delegate = null;

    public SimpleBlockingQueue() {
        delegate = new LinkedBlockingQueue();
    }

    public SimpleBlockingQueue( int capacity ) {
        delegate = new LinkedBlockingQueue( capacity );
    }
    
    public T peek() {
        return delegate.peek();
    }

    public T poll() {
        return delegate.poll();
    }

    public T poll( long timeout, TimeUnit unit ) {
        try {
            return delegate.poll( timeout, unit );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }
    
    public T take() {
        try {
            return delegate.take();
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }

    public void put( T value ) {
        try {
            delegate.put( value );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }

    public Iterator<T> iterator() {
        return delegate.iterator();
    }
    
    public void putWhenMissing( T value ) {

        if ( delegate.contains( value ) == false )
            put( value );
        
    }

    public int remainingCapacity() {
        return delegate.remainingCapacity();
    }
    
    public int size() {
        return delegate.size();
    }
    
    @Override
    public String toString() {
        return delegate.toString();
    }
    
}
