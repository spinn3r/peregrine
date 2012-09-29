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
    public static boolean TRACE = true; //FIXME
    
    /**
     * The current amount of memory allocated.
     */
    private AtomicLong allocated = new AtomicLong();

    protected long capacity = -1;

    public long get() {
        return allocated.get();
    }

    public void incr( long v ) {

        if ( TRACE ) {
            log.info( String.format( "%s and incr() by %,d bytes.", getStatus(), v ) );
        }
            
        allocated.getAndAdd( v );

    }

    public void decr( long v ) {

        if ( TRACE ) {
            log.info( String.format( "%s and decr() by %,d bytes.", getStatus(), v ) );
        }

        allocated.getAndAdd( -1 * v );

    }

    private String getStatus() {

        long current = get();

        if ( capacity > 0 ) {
        
            int perc = (int)(100 * ((double)current / (double)capacity));
            
            return String.format( "Allocated %,d with capacity %,d (%s %%)", current, capacity, perc );

        } else {
            return String.format( "Allocated %,d (unknown capacity)", current );
        }
            
    }
    
    /**
     * Not that we failed to allocate memory.
     */
    public void fail( long v, Throwable cause ) {
        if ( TRACE ) {
            log.error( String.format( "%s and FAILED to allocate by %,d bytes.", getStatus(), v ), cause );
        }
    }

    public String toString() {
        return String.format( "%s %s", getClass().getSimpleName(), getStatus() );
    }
    
}

