
package peregrine.util;

import java.util.concurrent.*;

/**
 * Uninterruptable version of LinkedBlockingQueue.  Code should not be calling
 * interrrupt() on us and hence these InterrruptExceptions are pointless.
 */
public class SimpleBlockingQueue<T> {

    LinkedBlockingQueue<T> delegate = new LinkedBlockingQueue();

    public SimpleBlockingQueue() { }

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

    public int size() {
        return delegate.size();
    }
    
    @Override
    public String toString() {
        return delegate.toString();
    }
    
}
