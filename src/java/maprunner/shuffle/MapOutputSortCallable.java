
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

public class MapOutputSortCallable implements Callable {

    private MapOutputIndex mapOutputIndex = null;
    
    public MapOutputSortCallable( MapOutputIndex mapOutputIndex ) {
        this.mapOutputIndex = mapOutputIndex;
    }

    public Object call() throws Exception {

        //FIXME: this implements the DEFAULT sort everything approach not the
        //hinted pre-sorted approach.
        
        Collection<MapOutputBuffer> mapOutput = mapOutputIndex.getMapOutput();

        int size = 0;
        
        for ( MapOutputBuffer mapOutputBuffer : mapOutput ) {
            size += mapOutputBuffer.size();
        }

        List<Tuple[]> arrays = new ArrayList();

        int nr_tuples = 0;
        for ( MapOutputBuffer mapOutputBuffer : mapOutput ) {

            Tuple[] copy = mapOutputBuffer.toArray();

            //TODO: shortcut this if the map output is already sorted.
            Arrays.sort( copy );
            nr_tuples += copy.length;

            arrays.add( copy );
            
        }

        System.out.printf( "Sorted %,d entries for partition %s \n", nr_tuples , mapOutputIndex.partition );
        
        return null;

    }

    public static SortResult sort( Tuple[] vect_left,
                                   Tuple[] vect_right ) {

        SortInput left = new SortInput( vect_left );
        SortInput right = new SortInput( vect_right );

        SortResult result = new SortResult( vect_left.length + vect_right.length );
        
        while( true ) {

            SortInput hit = null;
            SortInput miss = null;
            
            if ( left.value.compareTo( right.value ) <= 0 ) {

                hit = left;
                miss = right;
                                
            } else {

                hit = right;
                miss = left;

            }

            result.accept( hit.value );
            
            ++hit.idx;

            if ( hit.idx == hit.vect.length ) {
                //drain the data from the 'miss' so that there are no more
                //entries in it.

                for( int i = miss.idx; i < miss.vect.length; ++i ) {
                    result.accept( hit.value );
                }
                
                break;
            }

            hit.value = hit.vect[ hit.idx ];
            
        }

        result.dump();
        
        return result;

    }

}

final class SortInput {

    public Tuple value;
    public int idx = 0;
    public Tuple[] vect;
    
    public SortInput( Tuple[] vect ) {
        this.vect = vect;
        this.value = vect[0];
    }

}

final class SortEntry extends ArrayList<byte[]> {

    public Tuple tuple;

    public SortEntry( Tuple tuple ) {
        this.tuple = tuple;
    }
    
}
