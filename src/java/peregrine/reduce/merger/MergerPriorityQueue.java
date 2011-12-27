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
import peregrine.io.chunk.*;
import peregrine.reduce.*;

public class MergerPriorityQueue {

    private PriorityQueue<MergeQueueEntry> queue = null;

    protected int key_offset = 0;

    protected ChunkMergeComparator comparator = new ChunkMergeComparator();
    
    public MergerPriorityQueue( List<ChunkReader> readers ) throws IOException {

        this.queue = new PriorityQueue( readers.size(), comparator );
        
        for( ChunkReader reader : readers ) {

            if ( reader.hasNext() == false )
                continue;
            
            MergeQueueEntry entry = new MergeQueueEntry( reader );

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

            // add this back in with the next value.
            entry.setKey( entry.reader.key() );
            entry.setValue( entry.reader.value() );
            
            add( entry );
            
        }

        return result;
        
    }
    
    private void add( MergeQueueEntry entry ) {
        queue.add( entry );
    }

}

class ChunkMergeComparator implements Comparator<MergeQueueEntry> {

    //private DepthBasedKeyComparator delegate = new DepthBasedKeyComparator();
    private FullKeyComparator delegate = new FullKeyComparator();
    
    public int compare( MergeQueueEntry k0, MergeQueueEntry k1 ) {
        return delegate.compare( k0.keyAsByteArray, k1.keyAsByteArray );
    }

}

