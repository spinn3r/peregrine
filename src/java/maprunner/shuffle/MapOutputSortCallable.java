
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

            //TODO: I'm not sure copying is the right solution.
            Tuple[] copy = mapOutputBuffer.toArray();

            //TODO: shortcut this if the map output is already sorted.
            Arrays.sort( copy );
            nr_tuples += copy.length;

            arrays.add( copy );
            
        }

        System.out.printf( "Sorted %,d entries for partition %s \n", nr_tuples , mapOutputIndex.partition );
        
        return null;

    }

    public static SortRecord[] sort( List<SortRecord[]> input ) {

        // we're done.
        if ( input.size() == 1 )
            return input.get( 0 );

        List<SortRecord[]> result = new ArrayList();
        
        for( int i = 0; i < input.size(); ++i ) {

            SortRecord[] left  = input.get( i );
            SortRecord[] right = input.get( ++i );

            result.add( sort( left, right ) );
            
        }

        //FIXME: include the odd man out in this set.

        return sort( result );
            
    }
    
    public static SortRecord[] sort( SortRecord[] vect_left,
                                     SortRecord[] vect_right ) {

        SortInput left = new SortInput( vect_left );
        SortInput right = new SortInput( vect_right );

        SortMerger merger = null;
        boolean raw = false;

        if ( left.value instanceof Tuple ) {
            raw = true;
            merger = new SortMerger();
        } else {
            merger = new IntermediateMerger();
        }

        SortResult result = new SortResult( vect_left.length + vect_right.length, merger, raw );

        while( true ) {

            SortInput hit = null;
            SortInput miss = null;

            long cmp = left.value.longValue() - right.value.longValue();

            if ( cmp <= 0 ) {
                hit = left;
                miss = right;
            } else {
                hit = right;
                miss = left;
            }

            result.accept( cmp, hit.value );
            
            ++hit.idx;

            if ( hit.idx == hit.vect.length ) {
                //drain the data from the 'miss' so that there are no more
                //entries in it.

                for( int i = miss.idx; i < miss.vect.length; ++i ) {
                    result.accept( -1 , miss.value );
                }
                
                break;
            }

            hit.value = hit.vect[ hit.idx ];
            
        }

        SortRecord[] records = result.getRecords();
        result.dump( records );
        
        return records;

    }

}

final class SortInput {

    public SortRecord value;
    public int idx = 0;
    public SortRecord[] vect;
    
    public SortInput( SortRecord[] vect ) {
        this.vect = vect;
        this.value = vect[0];
    }

}

class IntermediateMerger extends SortMerger {
    
    public void merge( SortEntry entry, SortRecord record ) {
        entry.values.addAll( ((SortEntry)record).values );
    }

    public SortEntry newSortEntry( SortRecord record ) {

        SortEntry template = (SortEntry) record;
        SortEntry result = new SortEntry();
        result.keycmp = template.keycmp;
        result.key = template.key;

        return result;
        
    }

}