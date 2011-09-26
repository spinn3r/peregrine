
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

//FIXME: rename this to ShuffleSortCallable
public class MapOutputSortCallable implements Callable {

    private MapOutputIndex mapOutputIndex = null;
    private final Reducer reducer;

    private boolean triggerReducer = false;
    
    public MapOutputSortCallable( MapOutputIndex mapOutputIndex,
                                  Class reducer_class )
        throws ExecutionException {
        
        this.mapOutputIndex = mapOutputIndex;

        try { 
            this.reducer = (Reducer)reducer_class.newInstance();
        } catch ( Exception e ) {
            throw new ExecutionException( e );
        }

    }

    public Object call() throws Exception {

        //FIXME: this implements the DEFAULT sort everything approach not the
        //hinted pre-sorted approach.

        this.reducer.init( mapOutputIndex.partition );
        
        Collection<MapOutputBuffer> mapOutputBuffers = mapOutputIndex.getMapOutput();

        int size = 0;
        
        for ( MapOutputBuffer mapOutputBuffer : mapOutputBuffers ) {
            size += mapOutputBuffer.size();
        }

        int nr_tuples = 0;

        for ( MapOutputBuffer mapOutputBuffer : mapOutputBuffers ) {

            
            
        }

        Sorter sorter = new Sorter( new SortListener() {

                public void onFinalValue( byte[] key, List<byte[]> values ) {
                    reducer.reduce( key, values );
                }
                
            } );

        //FIXME refactor: this needs to back in but using the new ChunkReader /
        //ChunkWriter sort mechanism

        //SortRecord[] sorted = sorter.sort( arrays );

        System.out.printf( "Sorted %,d entries for partition %s \n", nr_tuples , mapOutputIndex.partition );

        this.reducer.cleanup();

        return null;

    }

}
