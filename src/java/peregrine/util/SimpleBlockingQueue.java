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

import java.util.*;
import java.util.concurrent.*;

/**
 * Uninterruptable version of LinkedBlockingQueue.  Code should not be calling
 * interrrupt() on us and hence these InterrruptExceptions are pointless.
 */
public class SimpleBlockingQueue<T> {

    BlockingQueue<T> delegate = null;

    private long timeout = -1;
    
    public SimpleBlockingQueue() {
        delegate = new LinkedBlockingQueue();
    }

    public SimpleBlockingQueue( int capacity ) {
        this( capacity, -1 );
    }

    public SimpleBlockingQueue( int capacity, long timeout ) {
        delegate = new LinkedBlockingQueue( capacity );
        this.timeout = timeout;
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

            if ( timeout <= 0 ) {
                delegate.put( value );
            } else if ( delegate.offer( value, timeout, TimeUnit.MILLISECONDS ) == false ) {
                throw new RuntimeException( "Timeout: " + timeout );
            }

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
