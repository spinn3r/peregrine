/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
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

    BlockingQueue<Entry<T,E>> delegate = null;

    public SimpleBlockingQueueWithFailure() {
        delegate = new LinkedBlockingQueue();
    }

    public SimpleBlockingQueueWithFailure( int capacity ) {
        delegate = new ArrayBlockingQueue( capacity );
    }
    
    public T peek() throws E {
        return handleResult( delegate.peek() );
    }

    public T poll() throws E {
        return handleResult( delegate.poll() );
    }

    public T take() throws E {
        try {
            return handleResult( delegate.take() );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
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
