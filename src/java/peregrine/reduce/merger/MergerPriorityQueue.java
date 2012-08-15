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
package peregrine.reduce.merger;

import java.io.*;
import java.util.*;

import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.reduce.*;

public class MergerPriorityQueue {

    private PriorityQueue<MergeQueueEntry> queue = null;

    protected int key_offset = 0;

    protected ChunkMergeComparator comparator = null;
    
    public MergerPriorityQueue( List<SequenceReader> readers,
                                ReduceComparator reduceComparator ) throws IOException {

        this.comparator = new ChunkMergeComparator( reduceComparator );
        
        if ( readers.size() == 0 )
            throw new IllegalArgumentException( "readers" );
    	
        this.queue = new PriorityQueue( readers.size(), comparator );
        
        for( int id = 0; id < readers.size(); ++id  ) {

            SequenceReader reader = readers.get( id );
                        
            if ( reader.hasNext() == false )
                continue;
            
            MergeQueueEntry entry = new MergeQueueEntry( reader, id );
            
            queue.add( entry );
            
        }
        
    }

    public MergeQueueEntry poll() throws IOException {

        //TODO: there's an optimization here where we get down to only one
        //priority queue as we can just return from it directly.
        
        MergeQueueEntry entry = queue.poll();

        if ( entry == null )
            return null;

        MergeQueueEntry result = entry.copy();
        
        if ( entry.reader.hasNext() ) {

        	entry.reader.next();
        	
            // add this back in with the next value.
            entry.setKey( entry.reader.key() );
            entry.setValue( entry.reader.value() );
            
            queue.add( entry );
            
        }

        return result;
        
    }

}

