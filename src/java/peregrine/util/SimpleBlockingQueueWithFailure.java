
package peregrine.util;

import java.util.concurrent.*;

/**
 * Version of SimpleBlockingQueue which allows the producer to send exception up
 * into the consumers so that they can then throw these to callers.
 * 
 * This is useful when the producer has failed so that Throwables can make it
 * into your code.
 * 
 */
public class SimpleBlockingQueueWithFailure<T,E extends Exception> {

    LinkedBlockingQueue<Entry<T,E>> delegate = new LinkedBlockingQueue();

    public SimpleBlockingQueueWithFailure() { }

    public SimpleBlockingQueueWithFailure( int capacity ) {
        delegate = new LinkedBlockingQueue( capacity );
    }
    
    public T peek() throws E {
        return handleResult( delegate.peek() );
    }

    public T poll() throws E {

        return handleResult( delegate.poll() );

    }

    private T handleResult( Entry<T,E> entry ) throws E {

        if ( entry == null )
            return null; // for peek this is acceptable

        if ( entry.cause != null ) {
            put( entry ); 
            throw entry.cause;
        }
        
        return entry.value;

    }
    
    public void put( T value ) {
        put( new Entry( value ) );
    }

    protected void put( Entry<T,E> value ) {
        try {
            delegate.put( value );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }

    public void raise( E cause ) {
        put( new Entry( cause ) );
    }
    
    public int size() {
        return delegate.size();
    }
    
    @Override
    public String toString() {
        return delegate.toString();
    }

    class Entry<T,E extends Exception> {

        T value;
        E cause;

        public Entry( T value ) {
            this.value = value;
        }

        public Entry( E cause ) {
            this.cause = cause;
        }

    }

}
