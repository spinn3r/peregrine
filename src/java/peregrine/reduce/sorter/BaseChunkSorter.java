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
package peregrine.reduce.sorter;

import java.io.*;
import java.util.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.driver.shuffle.*;
import peregrine.io.util.*;
import peregrine.reduce.*;
import peregrine.util.*;

import org.jboss.netty.buffer.*;

/**
 * Base sorter which includes the core logic behind sorting.
 * 
 */
public class BaseChunkSorter {

    private ReduceComparator comparator;

    public BaseChunkSorter( ReduceComparator comparator ) {
        this.comparator = comparator;
    }

    public KeyLookup sort( KeyLookup input ) 
        throws IOException {

        int depth = 0;
        
        return sort( input, depth );

    }

    protected KeyLookup sort( KeyLookup input, int depth )
        throws IOException {

        if ( input.size() <= 1 ) {
            return input;
        }

        int middle = input.size() / 2; 

        KeyLookup left  = null;
        KeyLookup right = null;

        try {

            left  = input.slice( 0, middle - 1 );
            right = input.slice( middle, input.size() - 1 );

            left  = sort( left  , depth + 1 );
            right = sort( right , depth + 1 );

            // determine which merge structure to use... if this is the LAST merger
            // just write the results to disk.  Writing to memory and THEN writing
            // to disk would just waste CPU time.

            return merge( left, right, depth );
            
        } finally {

            // close both the left and right inputs to return their memory.
            new Closer( left, right, input ).close();

        }
        
    }

    private KeyLookup merge( KeyLookup left,
                             KeyLookup right,
                             int depth )
        throws IOException {
        
        KeyLookup result = new KeyLookup( left.size() + right.size(), left.buffers );
        
        // now merge both left and right and we're done.
        List<KeyLookup> list = new ArrayList( 2 );
        list.add( left );
        list.add( right );

        SorterPriorityQueue queue = new SorterPriorityQueue( list, comparator );

        while( true ) {
            
            SortQueueEntry poll = queue.poll();

            if ( poll == null )
                break;
            
            result.next();
            result.set( poll.entry );
            
        }

        result.reset();
        return result;
        
    }

}
