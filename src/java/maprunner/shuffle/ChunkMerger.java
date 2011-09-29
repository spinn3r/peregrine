
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.io.*;

/**
 * http://en.wikipedia.org/wiki/External_sorting
 * 
 * One example of external sorting is the external merge sort algorithm, which
 * sorts chunks that each fit in RAM, then merges the sorted chunks
 * together.[1][2] For example, for sorting 900 megabytes of data using only 100
 * megabytes of RAM:
 * 
 * Read 100 MB of the data in main memory and sort by some conventional method,
 * like quicksort.
 * 
 * Write the sorted data to disk.
 * 
 * Repeat steps 1 and 2 until all of the data is in sorted 100 MB chunks (there are
 * 900MB / 100MB = 9 chunks), which now need to be merged into one single output
 * file.
 * 
 * Read the first 10 MB (= 100MB / (9 chunks + 1)) of each sorted chunk into input
 * buffers in main memory and allocate the remaining 10 MB for an output
 * buffer. (In practice, it might provide better performance to make the output
 * buffer larger and the input buffers slightly smaller.)
 * 
 * Perform a 9-way merge and store the result in the output buffer. If the output
 * buffer is full, write it to the final sorted file, and empty it. If any of the 9
 * input buffers gets empty, fill it with the next 10 MB of its associated 100 MB
 * sorted chunk until no more data from the chunk is available. This is the key
 * step that makes external merge sort work externally -- because the merge
 * algorithm only makes one pass sequentially through each of the chunks, each
 * chunk does not have to be loaded completely; rather, sequential parts of the
 * chunk can be loaded as needed.
 * 
 * http://en.wikipedia.org/wiki/Merge_algorithm
 * 
 * Merge algorithms generally run in time proportional to the sum of the lengths of
 * the lists; merge algorithms that operate on large numbers of lists at once will
 * multiply the sum of the lengths of the lists by the time to figure out which of
 * the pointers points to the lowest item, which can be accomplished with a
 * heap-based priority queue in O(log n) time, for O(m log n) time, where n is the
 * number of lists being merged and m is the sum of the lengths of the lists. When
 * merging two lists of length m, there is a lower bound of 2m − 1 comparisons
 * required in the worst case.
 * 
 * The classic merge (the one used in merge sort) outputs the data item with the
 * lowest key at each step; given some sorted lists, it produces a sorted list
 * containing all the elements in any of the input lists, and it does so in time
 * proportional to the sum of the lengths of the input lists.
 * 
 * 
 */
public class ChunkMerger {

    public static int DEFAULT_PARTITION_WIDTH = 1000;

    private SortListener listener = null;

    private IntermediateChunkHelper intermediateChunkHelper
        = new IntermediateChunkHelper();

    private SortEntryFactory defaultSortEntryFactory = new DefaultSortEntryFactory();

    public int tuples = 0;
        
    private SortEntryFactory topLevelSortEntryFactory = new TopLevelSortEntryFactory();
    
    public ChunkMerger() {
    }

    public ChunkMerger( SortListener listener ) {
        this.listener = listener;
    }

    public void merge( List<ChunkReader> input ) throws IOException {
        merge( input, null );
    }
    
    public void merge( List<ChunkReader> input, ChunkWriter writer ) throws IOException {

        //FIXME: if the input length is zero or one then we are done.

        PartitionPriorityQueue queue = new PartitionPriorityQueue( input );
        
        SortListener sortListener = null;

        if ( input.size() <= DEFAULT_PARTITION_WIDTH ) 
            sortListener = listener;
            
        SortResult result = new SortResult( writer, sortListener );

        while( true ) {
            
            PartitionPriorityQueueEntry entry = queue.poll();

            if ( entry == null )
                break;

            ++tuples;
            
            result.accept( entry.cmp, topLevelSortEntryFactory.newSortEntry( entry.t ) );

        }

        result.close();

        if ( writer != null )         
            writer.close();
        
    }

}

class PartitionPriorityQueue {

    private PriorityQueue<PartitionPriorityQueueEntry> queue = null;

    protected int key_offset = 0;

    protected PartitionPriorityQueueEntry result = new PartitionPriorityQueueEntry();
    
    public PartitionPriorityQueue( List<ChunkReader> readers ) throws IOException {

        this.queue = new PriorityQueue( readers.size() );
        
        for( ChunkReader reader : readers ) {

            Tuple t = reader.read();

            if ( t == null )
                continue;
            
            PartitionPriorityQueueEntry entry = new PartitionPriorityQueueEntry();
            entry.reader = reader;
            entry.t = t;
            entry.queue = this;

            queue.add( entry );
            
        }
        
    }

    public PartitionPriorityQueueEntry poll() throws IOException {

        //TODO: there's an optimization here where we get down to only one
        //priority queue as we can just return from it directly.
        
        PartitionPriorityQueueEntry entry = queue.poll();

        if ( entry == null )
            return null;

        this.result.t = entry.t;
        this.result.cmp = entry.cmp;
        
        Tuple t = entry.reader.read();

        if ( t != null ) {
            // add this back in with the next value.. 
            entry.t = t;
            add( entry );
        }

        return this.result;
        
    }
    
    private void add( PartitionPriorityQueueEntry entry ) {
        queue.add( entry );
    }

}

class PartitionPriorityQueueEntry implements Comparable<PartitionPriorityQueueEntry> {

    public Tuple t = null;

    public int cmp;
    
    protected PartitionPriorityQueue queue = null;

    protected ChunkReader reader = null;

    public int compareTo( PartitionPriorityQueueEntry p ) {

        //FIXME: merge this with FileComparator so that they use the SAME code
        //...  It's essentially the SAME algorithm.... this will remove bugs and
        //limit the amount of code.
        
        while( queue.key_offset < t.key.length ) {

            this.cmp = t.key[queue.key_offset] - p.t.key[queue.key_offset];

            if ( this.cmp != 0 || queue.key_offset == t.key.length - 1 ) {
                return this.cmp;
            }

            ++queue.key_offset;

        }

        return this.cmp;
        
    }
    
}