
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
 */
public class ChunkSorter {

    public static int DEFAULT_PARTITION_WIDTH = 25;

    private IntermediateChunkHelper intermediateChunkHelper
        = new IntermediateChunkHelper();

    private SortEntryFactory defaultSortEntryFactory = new DefaultSortEntryFactory();

    private SortEntryFactory topLevelSortEntryFactory = new TopLevelSortEntryFactory();

    public ChunkReader sort( ChunkReader input,
                             ChunkWriter writer ) throws IOException {

        //FIXME: this is FAR from optimal right now but lets benchmark it...
        //
        // - it requires too mucn array maintenance
        // - requires a final pass at the end to write the values
        // - a hand tuned version would be better.
        
        int size = input.size();

        //List<Tuple> list = new ArrayList( size ); //FIXME: add this back in... it's broken for now.
        List<Tuple> list = new ArrayList();

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

        for( Tuple t : list ) {
            writer.write( t.key, t.value );
        }

        writer.close();
        
        return result;
        
    }

}
