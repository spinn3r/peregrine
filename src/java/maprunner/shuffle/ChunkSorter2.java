
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
 * 
 * 
 */
public class ChunkSorter2 {

    public static int DEFAULT_PARTITION_WIDTH = 25;
    
    private boolean finalPass = false;

    private SortListener listener = null;

    private IntermediateChunkHelper intermediateChunkHelper
        = new IntermediateChunkHelper();

    private SortEntryFactory defaultSortEntryFactory = new DefaultSortEntryFactory();

    private SortEntryFactory topLevelSortEntryFactory = new TopLevelSortEntryFactory();

    private boolean debug = false;
    
    public ChunkSorter2() {
    }

    public ChunkSorter2( SortListener listener ) {
        this.listener = listener;
    }
    
    public void sort( List<ChunkReader> input ) throws IOException {
        sort( input, topLevelSortEntryFactory );
    }
    
    private void sort( List<ChunkReader> input,
                       SortEntryFactory sortEntryFactory ) throws IOException {

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
