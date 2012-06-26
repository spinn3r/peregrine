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

/**
 * Track memory allocations.
 */
public class MemoryAllocationTracker {

    /**
     * True when we should trace memory allocations.
     */
    public static boolean TRACE = true;
    
    /**
     * The current amount of memory allocated.
     */
    public AtomicLong allocated = new AtomicLong();

    public long get() {
        return allocated.get();
    }

    public void incr( long v ) {

        if ( TRACE ) {
            log.info( "Current capacity is %,d and incrementing by %,d", get(), v );
        }
            
        allocated.getAndAdd( v );

    }

    public void decr( long v ) {

        if ( TRACE ) {
            log.info( "Current capacity is %,d and about to decrement by %,d", get(), v );
        }

        incr( -1 * v );
    }
    
}

