
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
        
        Collection<MapOutputBuffer> mapOutputBuffers = mapOutputIndex.getMapOutput();

        List<ChunkReader> sorted = new ArrayList();

        ChunkSorter sorter = new ChunkSorter();
        
        for ( MapOutputBuffer mapOutputBuffer : mapOutputBuffers ) {
            sorted.add( sorter.sort( mapOutputBuffer.getChunkReader() ) );
        }

        final AtomicInteger nr_tuples = new AtomicInteger();

        ChunkMerger merger = new ChunkMerger( new SortListener() {

                public void onFinalValue( byte[] key, List<byte[]> values ) {
                    reducer.reduce( key, values );
                    nr_tuples.getAndIncrement();
                }
                
            } );

        merger.merge( sorted );

        System.out.printf( "Sorted %,d entries for partition %s \n", nr_tuples.get() , mapOutputIndex.partition );

        this.reducer.cleanup();

        return null;

    }

}
