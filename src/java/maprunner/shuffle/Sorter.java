
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

        List<ChunkReader> intermediate = new ArrayList();
        
        for( int i = 0; i < input.size() / 2; ++i ) {

            int offset = i * 2;
            
            ChunkReader left  = input.get( offset );
            ChunkReader right = input.get( ++offset );

            ChunkWriter writer = intermediateChunkHelper.getChunkWriter();
            
            sort( left, right, writer );

            intermediate.add( intermediateChunkHelper.getChunkReader() );
            
            
        }

        //FIXME: no recursion just yet because we need to find a way to keep
        //intermediate files.
        //sort( result );
            
    }
    
    private void sort( ChunkReader vect_left,
                       ChunkReader vect_right,
                       ChunkWriter writer ) throws IOException {

        SortInput left = new SortInput( vect_left );
        SortInput right = new SortInput( vect_right );

        SortListener sortListener = null;

        if ( finalPass ) 
            sortListener = listener;
            
        SortResult result = new SortResult( writer, sortListener );

        while( true ) {
            
            //FIXME: this won't work if one of the inputs is empty.

            SortInput hit = null;
            SortInput miss = null;

            long cmp = left.entry.cmp( right.entry );

            System.out.printf( "FIXME: left(%s) vs right(%s) = %d\n", Base64.encode( left.entry.key ), Base64.encode( right.entry.key ), cmp );
            
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