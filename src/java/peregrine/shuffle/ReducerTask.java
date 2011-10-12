
package peregrine.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;

public class ReducerTask extends BaseOutputTask implements Callable {
    
    private Input input = null;

    private MapOutputIndex mapOutputIndex = null;

    private Reducer reducer;

    private Class reducer_class = null;

    private Host host = null;
    
    public ReducerTask( Config config,
                        MapOutputIndex mapOutputIndex,
                        Host host,
                        Class reducer_class )
        throws Exception {

        super.init( mapOutputIndex.partition );

        this.config = config;
        this.mapOutputIndex = mapOutputIndex;
        this.host = host;
        this.reducer_class = reducer_class;

    }

    public Object call() throws Exception {

        if ( output.getReferences().size() == 0 )
            throw new IOException( "Reducer tasks require output." );

        this.reducer = (Reducer)reducer_class.newInstance();

        try {

            setup();

            reducer.setBroadcastInput( BroadcastInputFactory.getBroadcastInput( config, getInput(), partition, host ) );

            reducer.init( getJobOutput() );

            doCall();

        } finally {

            reducer.cleanup();
            teardown();

        }

        return null;

    }

    private void doCall() throws Exception {

        //FIXME: this implements the DEFAULT sort everything approach not the
        //hinted pre-sorted approach which in some applications would be MUCH
        //faster for the reduce operation.

        final AtomicInteger nr_tuples = new AtomicInteger();

        ReducerTaskSortListener listener =
            new ReducerTaskSortListener( reducer );
        
        LocalReducer reducer = new LocalReducer( listener );
        
        Collection<MapOutputBuffer> mapOutputBuffers = mapOutputIndex.getMapOutput();

        int nr_readers = 0;
        for ( MapOutputBuffer mapOutputBuffer : mapOutputBuffers ) {
            reducer.add( mapOutputBuffer.getChunkReader() );
            ++nr_readers;
        }

        reducer.sort();

        System.out.printf( "Sorted %,d entries in %,d chunk readers for partition %s \n",
                           listener.nr_tuples , nr_readers, mapOutputIndex.partition );

        // we have to close ALL of our output streams now.

    }

    public void setInput( Input input ) { 
        this.input = input;
    }

    public Input getInput() { 
        return this.input;
    }

}

class ReducerTaskSortListener implements SortListener {

    private Reducer reducer = null;

    public int nr_tuples = 0;
    
    public ReducerTaskSortListener( Reducer reducer ) {
        this.reducer = reducer;
    }
    
    public void onFinalValue( byte[] key, List<byte[]> values ) {

        try {

            reducer.reduce( key, values );
            ++nr_tuples;

        } catch ( Exception e ) {
            throw new RuntimeException( "Reduce failed: " , e );
        }
            
    }

}