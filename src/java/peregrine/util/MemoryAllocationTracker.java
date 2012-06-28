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
import java.util.concurrent.atomic.*;

import peregrine.*;

import com.spinn3r.log5j.Logger;

/**
 * Track memory allocations.
 */
public class MemoryAllocationTracker {

    private static final Logger log = Logger.getLogger();

    /**
     * True when we should trace memory allocations.
     */
    public static boolean TRACE = true;
    
    /**
     * The current amount of memory allocated.
     */
    private AtomicLong capacity = new AtomicLong();

    public long get() {
        return capacity.get();
    }

    public void incr( long v ) {

        if ( TRACE ) {
            long current = get();
            log.info( String.format( "Capacity is %,d and incrementing by %,d bytes.", current, v ) );
        }
            
        capacity.getAndAdd( v );

    }

    public void decr( long v ) {

        if ( TRACE ) {
            long current = get();
            log.info( String.format( "Capacity is %,d and about to decrement by %,d bytes.", current, v ) );
        }

        capacity.getAndAdd( -1 * v );

    }

    /**
     * Not that we failed to allocate memory.
     */
    public void fail( long v, Throwable cause ) {
        if ( TRACE ) {
            log.error( String.format( "Capacity is %,d and FAILED to allocate by %,d bytes.", get(), v ), cause );
        }
    }

    public String toString() {
        return String.format( "%s capacity=%,d", getClass().getSimpleName(), capacity.get() );
    }
    
}

