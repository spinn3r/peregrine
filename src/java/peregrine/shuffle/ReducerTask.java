
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

import com.spinn3r.log5j.Logger;

public class ReducerTask extends BaseOutputTask implements Callable {

    private static final Logger log = Logger.getLogger();

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

            reducer.setBroadcastInput( BroadcastInputFactory.getBroadcastInput( config, getInput(), partition ) );

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

        String shuffle_dir = config.getShuffleDir( shuffleInput.getName() );

        log.info( "Trying to find shuffle files in: %s", shuffle_dir );

        File shuffle_dir_file = new File( shuffle_dir );

        if ( ! shuffle_dir_file.exists() ) {
            throw new IOException( "Shuffle output does not exist: " + shuffleInput.getName() );
        }

        File[] files = shuffle_dir_file.listFiles();

        for( File file : files ) {
            ChunkReader reader = new ShuffleInputChunkReader( file.getPath(), partition.getId() );

            reducer.add( reader );
        }
        
        int nr_readers = files.length;

        reducer.sort();

        log.info( "Sorted %,d entries in %,d chunk readers for partition %s",
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