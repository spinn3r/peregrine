
package peregrine.reduce.sorter;

import java.io.*;
import java.util.*;
import peregrine.util.*;
import org.jboss.netty.buffer.*;

public class SorterPriorityQueue {

    private PriorityQueue<SortQueueEntry> queue = null;

    protected int key_offset = 0;

    protected SortQueueEntry result = new SortQueueEntry();

    protected SortMergeComparator comparator = new SortMergeComparator();
    
    public SorterPriorityQueue( List<KeyLookup> lookups ) throws IOException {

        this.queue = new PriorityQueue( lookups.size(), comparator );
        
        for( KeyLookup lookup : lookups ) {

            if ( lookup.hasNext() == false )
                continue;

            lookup.next();
            SortQueueEntry entry = new SortQueueEntry();
            entry.lookup = lookup;
            entry.queue  = this;

            queue.add( entry );
            
        }
        
    }

    public SortQueueEntry poll() throws IOException {

        //TODO: there's an optimization here where we get down to only one
        //priority queue as we can just return from it directly.
        
        SortQueueEntry entry = queue.poll();

        if ( entry == null )
            return null;

        KeyLookup lookup = entry.lookup;

        // provide the lookup so we can debug this if necessary.
        this.result.lookup = lookup;

        //return a copy of the ptr so that we can read the ptr value.           
        this.result.ptr = lookup.get();
        
        if ( lookup.hasNext() ) {
            lookup.next();
            add( entry );
        }

        return this.result;
        
    }
    
    private void add( SortQueueEntry entry ) {
        queue.add( entry );
    }

}

class SortMergeComparator implements Comparator<SortQueueEntry> {

    public int compare( SortQueueEntry e0, SortQueueEntry e1 ) {

        KeyLookup lookup0 = e0.lookup;
        KeyLookup lookup1 = e1.lookup;
        
        ChannelBuffer buff0 = lookup0.buffer;
        ChannelBuffer buff1 = lookup1.buffer;
        
        int idx0 = lookup0.get();
        int idx1 = lookup1.get();
        
        int len = LongBytes.LENGTH;

        int diff = 0;

        for( int offset = 0; offset < len; ++offset ) {

            diff = buff1.getByte( idx0 + offset ) - buff1.getByte( idx1 + offset );

            if ( diff != 0 )
                return diff;
            
        }

        return diff;

    }

}

class SortQueueEntry {

    protected SorterPriorityQueue queue = null;

    protected KeyLookup lookup = null;

    /**
     * Used to store the offset pointer.
     */
    protected int ptr = 0;
    
}

