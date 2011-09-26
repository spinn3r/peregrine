
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

public class Sorter {

    private boolean finalPass = false;

    private SortListener listener = null;

    private IntermediateChunkHelper intermediateChunkHelper
        = new IntermediateChunkHelper();

    private boolean debug = false;
    
    public Sorter() {
    }

    public Sorter( SortListener listener ) {
        this.listener = listener;
    }

    public void sort( List<ChunkReader> input ) throws IOException {
        sort( input, 0 );
    }
    
    private void sort( List<ChunkReader> input, int depth ) throws IOException {

        // we're done... no more work to do because the number of chunks is one
        // and we can't merge anymore.

        if ( input.size() == 1 )
            return;

        SortEntryFactory sortEntryFactory = new DefaultSortEntryFactory();

        if ( depth == 0 )
            sortEntryFactory = new TopLevelSortEntryFactory();

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
                rightSortEntryFactory = new DefaultSortEntryFactory();
            }
            
            sort( left, right, leftSortEntryFactory, rightSortEntryFactory, writer );

            result.add( intermediateChunkHelper.getChunkReader() );

        }

        sort( result, ++depth );
            
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
            
            //FIXME: this won't work if one of the inputs is empty.

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
        return new ChunkReader( out.toByteArray() );
    }
    
}

interface SortEntryFactory  {
    
    public SortEntry newSortEntry( KeyValuePair keyValuePair );

}

class DefaultSortEntryFactory implements SortEntryFactory {
    
    public SortEntry newSortEntry( KeyValuePair keyValuePair ) {

        SortEntry entry = new SortEntry( keyValuePair.key );

        //after this we're merging arrays of values which are length prefixed.
        //FIXME: deserialzie these first.
        //entry.write( keyValuePair.value );

        ByteArrayListValue intervalue = new ByteArrayListValue();
        intervalue.fromBytes( keyValuePair.value );

        entry.addValues( intervalue.getValues() );
        
        return entry;

    }

}

class TopLevelSortEntryFactory implements SortEntryFactory {
    
    public SortEntry newSortEntry( KeyValuePair keyValuePair ) {

        VarintWriter writer = new VarintWriter();

        // the first value is a literal... 
        SortEntry entry = new SortEntry( keyValuePair.key );
        entry.addValue( keyValuePair.value );
        
        return entry;

    }

}