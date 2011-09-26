
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
    
    public Sorter() {
        this.listener = new SortListener(); // we don't care to listen.
    }

    public Sorter( SortListener listener ) {
        this.listener = listener;
    }

    public void sort( List<ChunkReader> input ) throws IOException {
        sort( input, 0 );
    }
    
    private void sort( List<ChunkReader> input, int depth ) throws IOException {

        // we're done.
        if ( input.size() == 1 )
            return;

        if ( input.size() == 2 ) {
            finalPass = true;
        }

//         //odd sized input.  First merge the last two.

//         if ( input.size() > 2 && input.size() % 2 != 0) {

//             //FIXME: include the odd man out in this set.

//             SortRecord[] left  = input.remove( input.size() - 2 );
//             SortRecord[] right = input.remove( input.size() - 1 );

//             input.add( sort( left, right ) );
            
//         }

        SortEntryFactory sortEntryFactory = new DefaultSortEntryFactory();

        if ( depth == 0 )
            sortEntryFactory = new TopLevelSortEntryFactory();
        
        List<ChunkReader> intermediate = new ArrayList();
        
        for( int i = 0; i < input.size() / 2; ++i ) {

            int offset = i * 2;
            
            ChunkReader left  = input.get( offset );
            ChunkReader right = input.get( ++offset );

            ChunkWriter writer = intermediateChunkHelper.getChunkWriter();
            
            sort( left, right, writer, sortEntryFactory );

            intermediate.add( intermediateChunkHelper.getChunkReader() );

        }

        sort( intermediate, ++depth );
            
    }
    
    private void sort( ChunkReader chunk_left,
                       ChunkReader chunk_right,
                       ChunkWriter writer,
                       SortEntryFactory sortEntryFactory ) throws IOException {

        SortInput left  = new SortInput( chunk_left , sortEntryFactory );
        SortInput right = new SortInput( chunk_right, sortEntryFactory );

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

        //deserialize the intermediate value.
        
        entry.values.add( keyValuePair.value );

        return entry;

    }

}

class TopLevelSortEntryFactory implements SortEntryFactory {
    
    public SortEntry newSortEntry( KeyValuePair keyValuePair ) {

        SortEntry entry = new SortEntry( keyValuePair.key );
        entry.values.add( keyValuePair.value );

        return entry;

    }

}