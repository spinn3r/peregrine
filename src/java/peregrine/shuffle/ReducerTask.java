
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
import peregrine.io.chunk.*;

import peregrine.pfsd.shuffler.ShuffleInputChunkReader;

public class ReducerTask extends BaseOutputTask implements Callable {
    
    private Input input = null;

    private Reducer reducer;

    private Class reducer_class = null;

    private ShuffleInputReference shuffleInput;

    private Host host = null;
    
    public ReducerTask( Config config,
                        Partition partition,
                        Class reducer_class,
                        ShuffleInputReference shuffleInput )
        throws Exception {

        super.init( partition );

        this.config = config;
        this.reducer_class = reducer_class;
        this.shuffleInput = shuffleInput;
        this.host = config.getHost();
        
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

        String shuffle_dir = config.getPFSPath( partition,
                                                config.getHost(),
                                                String.format( "/shuffle/%s/", shuffleInput.getName() ) );

        System.out.printf( "Trying to find suffle files in: %s\n", shuffle_dir );
        
        File[] files = new File( shuffle_dir ).listFiles();

        for( File file : files ) {
            ChunkReader reader = new ShuffleInputChunkReader( file.getPath(), partition.getId() );

            System.out.printf( "FIXME: %s for partition : %s\n", reader, partition.getId() );
            reducer.add( reader );
        }
        
        int nr_readers = files.length;

        reducer.sort();

        System.out.printf( "Sorted %,d entries in %,d chunk readers for partition %s \n",
                           listener.nr_tuples , nr_readers, partition );

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