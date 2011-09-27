
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
public class Sorter {

    private boolean finalPass = false;

    private SortListener listener = null;

    private IntermediateChunkHelper intermediateChunkHelper
        = new IntermediateChunkHelper();

    private SortEntryFactory defaultSortEntryFactory = new DefaultSortEntryFactory();

    private SortEntryFactory topLevelSortEntryFactory = new TopLevelSortEntryFactory();

    private boolean debug = false;
    
    public Sorter() {
    }

    public Sorter( SortListener listener ) {
        this.listener = listener;
    }

    /*
    public ChunkReader sort( ChunkReader input ) throws IOException {

        // what we could do here to minimize memory ... and not use too many
        // objects is to 

        int size = input.size();

        ChunkWriter writer = intermediateChunkHelper.getChunkWriter();

        if ( size == 0 )
            return intermediateChunkHelper.getChunkReader();

        if ( size == 1 ) {
            Tuple t = input.read();
            writer.write( t.key, t.value );
            return intermediateChunkHelper.getChunkReader();
        }

        SortEntryFactory sortEntryFactory = defaultSortEntryFactory;
        
        if ( size == 2 ) {
            sortEntryFactory = topLevelSortEntryFactory;
        }

    }
    */

    public ChunkReader sort( ChunkReader input ) throws IOException {

        ChunkWriter writer = intermediateChunkHelper.getChunkWriter();

        int size = input.size();

        List<Tuple> list = new ArrayList( size );

        // copy the values into the list...
        while ( true ) {

            Tuple t = input.read();

            if ( t == null )
                break;
            
            list.add( t );

        }

        Collections.sort( list , new Comparator<Tuple>() {

                public int compare( Tuple t0, Tuple t1 ) {

                    int len = t0.key.length;

                    for( int offset = 0; offset < len; ++offset ) {

                        int diff = t0.key[offset] - t1.key[offset];

                        if ( diff != 0 )
                            return diff;
                        
                    }

                    //we go to the end and there were no differences ....
                    return 0;

                }
                
            } );

        TupleListChunkReader result = new TupleListChunkReader( list );
        
        return result;
        
    }
    
    public void sort( List<ChunkReader> input ) throws IOException {
        sort( input, topLevelSortEntryFactory );
    }
    
    private void sort( List<ChunkReader> input,
                       SortEntryFactory sortEntryFactory ) throws IOException {

        // we're done... no more work to do because the number of chunks is one
        // and we can't merge anymore.

        if ( input.size() <= 1 )
            return;

        //odd sized input.  First merge the last two.

        List<ChunkReader> result = new ArrayList();

        boolean odd = false;
        
        if ( input.size() > 2 && input.size() % 2 != 0) {

            odd = true;
            
            ChunkWriter writer = intermediateChunkHelper.getChunkWriter();

            sort( input.remove( 0 ), input.remove( 1 ), sortEntryFactory, sortEntryFactory, writer );

            input.add( intermediateChunkHelper.getChunkReader() );

        }

        if ( input.size() == 2 ) {
            finalPass = true;
        }

        for( int i = 0; i < input.size() / 2; ++i ) {

            int offset = i * 2;

            int left_idx  = offset;
            int right_idx = left_idx + 1;
            
            ChunkReader left  = input.get( left_idx );
            ChunkReader right = input.get( right_idx );

            ChunkWriter writer = intermediateChunkHelper.getChunkWriter();

            SortEntryFactory leftSortEntryFactory = sortEntryFactory;
            SortEntryFactory rightSortEntryFactory = sortEntryFactory;

            // the is the last chunk ... 
            if ( odd && right_idx == input.size() - 1 ) {
                rightSortEntryFactory = defaultSortEntryFactory;
            }
            
            sort( left, right, leftSortEntryFactory, rightSortEntryFactory, writer );

            result.add( intermediateChunkHelper.getChunkReader() );

        }

        sort( result, defaultSortEntryFactory );
            
    }
    
    private void sort( ChunkReader chunk_left,
                       ChunkReader chunk_right,
                       SortEntryFactory leftSortEntryFactory,
                       SortEntryFactory rightSortEntryFactory,
                       ChunkWriter writer ) throws IOException {

        SortInput left  = new SortInput( chunk_left , leftSortEntryFactory );
        SortInput right = new SortInput( chunk_right, rightSortEntryFactory );

        SortListener sortListener = null;

        if ( finalPass ) 
            sortListener = listener;
            
        SortResult result = new SortResult( writer, sortListener );

        while( true ) {
            
            //NOTE: this won't work if one of the inputs is empty.  However, in
            //the next implementation this bug may go away.

            SortInput hit = null;
            SortInput miss = null;

            long cmp = left.entry.cmp( right.entry );

            if ( cmp <= 0 ) {
                hit = left;
                miss = right;
            } else {
                hit = right;
                miss = left;
            }

            result.accept( cmp, hit.entry );

            // make sure to page in the next value.
            hit.next();

            if ( hit.isExhausted() ) {

                //drain the data from the 'miss' so that there are no more
                //entries in it... because we are now done.

                while( ! miss.isExhausted() ) {
                    result.accept( -1 , miss.entry );
                    miss.next();
                }
                
                break;
            }

        }

        result.close();
        writer.close();
        
    }

}

class IntermediateChunkHelper {

    ByteArrayOutputStream out;

    public ChunkWriter getChunkWriter() throws IOException {

        this.out = new ByteArrayOutputStream();
        return new ChunkWriter( out );
        
    }

    public ChunkReader getChunkReader() throws IOException {
        return new DefaultChunkReader( out.toByteArray() );
    }
    
}

interface SortEntryFactory  {
    
    public SortEntry newSortEntry( Tuple tuple );

}

class DefaultSortEntryFactory implements SortEntryFactory {
    
    public SortEntry newSortEntry( Tuple tuple ) {

        SortEntry entry = new SortEntry( tuple.key );

        ByteArrayListValue intervalue = new ByteArrayListValue();
        intervalue.fromBytes( tuple.value );

        entry.addValues( intervalue.getValues() );
        
        return entry;

    }

}

class TopLevelSortEntryFactory implements SortEntryFactory {
    
    public SortEntry newSortEntry( Tuple tuple ) {

        VarintWriter writer = new VarintWriter();

        // the first value is a literal... 
        SortEntry entry = new SortEntry( tuple.key );
        entry.addValue( tuple.value );
        
        return entry;

    }

}