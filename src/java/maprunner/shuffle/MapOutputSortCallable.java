
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

        List<ChunkReader> sorted = new ArrayList();
        
        for ( MapOutputBuffer mapOutputBuffer : mapOutputBuffers ) {

            sorted.add( new Sorter().sort( mapOutputBuffer.getChunkReader() ) );
            
        }

        final AtomicInteger nr_tuples = new AtomicInteger();

        ChunkMerger merger = new ChunkMerger( new SortListener() {

                public void onFinalValue( byte[] key, List<byte[]> values ) {
                    reducer.reduce( key, values );
                    nr_tuples.getAndIncrement();
                }
                
            } );

        merger.merge( sorted );
        
        //FIXME refactor: this needs to back in but using the new ChunkReader /
        //ChunkWriter sort mechanism

        //SortRecord[] sorted = sorter.sort( arrays );

        System.out.printf( "Sorted %,d entries for partition %s \n", nr_tuples.get() , mapOutputIndex.partition );

        this.reducer.cleanup();

        return null;

    }

}
