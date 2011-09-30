
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.io.*;

public class MapOutputSortCallable implements Callable {

    private MapOutputIndex mapOutputIndex = null;
    private final Reducer reducer;

    private boolean triggerReducer = false;

    private String path;
    
    public MapOutputSortCallable( MapOutputIndex mapOutputIndex,
                                  Class reducer_class,
                                  String path )
        throws ExecutionException {
        
        this.mapOutputIndex = mapOutputIndex;
        this.path = path;

        try { 
            this.reducer = (Reducer)reducer_class.newInstance();
        } catch ( Exception e ) {
            throw new ExecutionException( e );
        }

    }

    public Object call() throws Exception {

        //FIXME: this implements the DEFAULT sort everything approach not the
        //hinted pre-sorted approach which in some applications would be MUCH
        //faster for the reduce operation.

        //FIXME: make this WHOLE thing testable externally ... 
        
        this.reducer.init( mapOutputIndex.partition, this.path );

        final AtomicInteger nr_tuples = new AtomicInteger();

        SortListener listener = new SortListener() {
    
                public void onFinalValue( byte[] key, List<byte[]> values ) {
                    reducer.reduce( key, values );
                    nr_tuples.getAndIncrement();
                }
                
            };
        
        MapOutputSorter sorter = new MapOutputSorter( listener );
        
        Collection<MapOutputBuffer> mapOutputBuffers = mapOutputIndex.getMapOutput();
        
        for ( MapOutputBuffer mapOutputBuffer : mapOutputBuffers ) {
            sorter.add( mapOutputBuffer.getChunkReader() );
        }

        sorter.sort();

        System.out.printf( "Sorted %,d entries for partition %s \n", nr_tuples.get() , mapOutputIndex.partition );

        this.reducer.cleanup();

        return null;

    }

}
