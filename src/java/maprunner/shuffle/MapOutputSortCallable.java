
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
    private final Reducer reducer;

    private boolean triggerReducer = false;
    
    public MapOutputSortCallable( MapOutputIndex mapOutputIndex,
                                  Reducer reducer ) {
        
        this.mapOutputIndex = mapOutputIndex;
        this.reducer = reducer;
        
    }

    public Object call() throws Exception {

        //FIXME: this implements the DEFAULT sort everything approach not the
        //hinted pre-sorted approach.
        
        Collection<MapOutputBuffer> mapOutput = mapOutputIndex.getMapOutput();

        int size = 0;
        
        for ( MapOutputBuffer mapOutputBuffer : mapOutput ) {
            size += mapOutputBuffer.size();
        }

        List<SortRecord[]> arrays = new ArrayList();

        int nr_tuples = 0;

        for ( MapOutputBuffer mapOutputBuffer : mapOutput ) {

            //TODO: I'm not sure copying is the right solution.
            Tuple[] copy = mapOutputBuffer.toArray();

            //TODO: shortcut this if the map output is already sorted.

            //FIXME: we should sort this by OUR function IMO.
            Arrays.sort( copy );

            nr_tuples += copy.length;

            arrays.add( (SortRecord[])copy );
            
        }

        Sorter sorter = new Sorter( new SortListener() {

                public void onFinalValue( byte[] key, List<byte[]> values ) {
                    reducer.reduce( key, values );
                }
                
            } );
        
        SortRecord[] sorted = sorter.sort( arrays );
        
        System.out.printf( "Sorted %,d entries for partition %s \n", nr_tuples , mapOutputIndex.partition );
        
        return null;

    }

}
