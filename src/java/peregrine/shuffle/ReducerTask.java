
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

    private MapOutputIndex mapOutputIndex = null;

    private Reducer reducer;

    private Class reducer_class = null;
    
    public ReducerTask( MapOutputIndex mapOutputIndex,
                        Class reducer_class,
                        Output output )
        throws Exception {

        super.init( mapOutputIndex.partition );
        
        this.mapOutputIndex = mapOutputIndex;
        this.reducer_class = reducer_class;
        
        setOutput( output );

    }

    public Object call() throws Exception {

        if ( output.getReferences().size() == 0 )
            throw new IOException( "Reducer tasks require output." );

        this.reducer = (Reducer)reducer_class.newInstance();

        try {

            setup();
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

        SortListener listener = new SortListener() {
    
                public void onFinalValue( byte[] key, List<byte[]> values ) {

                    try {
                        reducer.reduce( key, values );
                        nr_tuples.getAndIncrement();

                    } catch ( Exception e ) {
                        throw new RuntimeException( "Reduce failed: " , e );
                    }
                        
                }
                
            };
        
        LocalReducer reducer = new LocalReducer( listener );
        
        Collection<MapOutputBuffer> mapOutputBuffers = mapOutputIndex.getMapOutput();
        
        for ( MapOutputBuffer mapOutputBuffer : mapOutputBuffers ) {
            reducer.add( mapOutputBuffer.getChunkReader() );
        }

        reducer.sort();

        System.out.printf( "Sorted %,d entries for partition %s \n", nr_tuples.get() , mapOutputIndex.partition );

        // we have to close ALL of our output streams now.

    }

}

