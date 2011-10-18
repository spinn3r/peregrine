
package peregrine.reduce.merger;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.reduce.*;

public class MergerPriorityQueue {

    private PriorityQueue<MergeQueueEntry> queue = null;

    protected int key_offset = 0;

    protected MergeQueueEntry result = new MergeQueueEntry();

    protected ChunkMergeComparator comparator = new ChunkMergeComparator();
    
    public MergerPriorityQueue( List<ChunkReader> readers ) throws IOException {

        this.queue = new PriorityQueue( readers.size(), comparator );
        
        for( ChunkReader reader : readers ) {

            if ( reader.hasNext() == false )
                continue;
            
            MergeQueueEntry entry = new MergeQueueEntry();
            entry.reader = reader;

            entry.key = reader.key();
            entry.value = reader.value();

            entry.queue = this;

            queue.add( entry );
            
        }
        
    }

    public MergeQueueEntry poll() throws IOException {

        //TODO: there's an optimization here where we get down to only one
        //priority queue as we can just return from it directly.
        
        MergeQueueEntry entry = queue.poll();

        if ( entry == null )
            return null;

        this.result.key = entry.key;
        this.result.value = entry.value;

        if ( entry.reader.hasNext() ) {
            // add this back in with the next value.
            entry.key = entry.reader.key();
            entry.value = entry.reader.value();
            add( entry );
        }

        return this.result;
        
    }
    
    private void add( MergeQueueEntry entry ) {
        queue.add( entry );
    }

}

class ChunkMergeComparator implements Comparator<MergeQueueEntry> {

    //private DepthBasedKeyComparator delegate = new DepthBasedKeyComparator();
    private FullKeyComparator delegate = new FullKeyComparator();
    
    public int compare( MergeQueueEntry k0, MergeQueueEntry k1 ) {
        return delegate.compare( k0.key, k1.key );
    }

}

