
package peregrine.task;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import peregrine.*;
import peregrine.map.*;
import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.reduce.*;

import com.spinn3r.log5j.Logger;

public class ReducerTask extends BaseOutputTask implements Callable {

    private static final Logger log = Logger.getLogger();

    /**
     * When true, we should delete the on disk shuffle files after reduce runs
     * to free up disk space.  This should be enabled in production but in test
     * environments disabling this can enable us to run unit testing, etc.
     */
    public static boolean DELETE_SHUFFLE_FILES = false;
    
    private Input input = null;

    private Reducer reducer;

    private ShuffleInputReference shuffleInput;

    public ReducerTask( Config config,
                        Partition partition,
                        Class delegate,
                        ShuffleInputReference shuffleInput )
        throws Exception {

        super.init( partition );

        this.config = config;
        this.delegate = delegate;
        this.shuffleInput = shuffleInput;
        
    }

    public Object call() throws Exception {

        if ( output.getReferences().size() == 0 )
            throw new IOException( "Reducer tasks require output." );

        this.reducer = (Reducer)delegate.newInstance();

        try {

            setup();

            reducer.setBroadcastInput( BroadcastInputFactory.getBroadcastInput( config, getInput(), partition ) );

            reducer.init( getJobOutput() );

            try {
                doCall();
            } finally {
                reducer.cleanup();
            }

            setStatus( TaskStatus.COMPLETE );
            
        } catch ( Throwable t ) { 

            log.error( "Task failed for partition: " + partition, t );
            setStatus( TaskStatus.FAILED );
            setCause( t );

        } finally {

            teardown();

        }

        return null;

    }

    private void doCall() throws Exception {

        ReducerTaskSortListener listener =
            new ReducerTaskSortListener( reducer );
        
        LocalReducer reducer = new LocalReducer( config, partition, listener, shuffleInput );

        String shuffle_dir = config.getShuffleDir( shuffleInput.getName() );

        log.info( "Trying to find shuffle files in: %s", shuffle_dir );

        File shuffle_dir_file = new File( shuffle_dir );

        if ( ! shuffle_dir_file.exists() ) {
            throw new IOException( "Shuffle output does not exist: " + shuffleInput.getName() );
        }

        File[] shuffles = shuffle_dir_file.listFiles();

        //TODO: we should probably make sure these look like shuffle files.
        for( File shuffle : shuffles ) {
            reducer.add( shuffle );
        }
        
        int nr_readers = shuffles.length;

        reducer.sort();

        log.info( "Sorted %,d entries in %,d chunk readers for partition %s",
                  listener.nr_tuples , nr_readers, partition );

        // FIXME: we can't delete these until ALL reduces are done because we
        // may do speculative execution and need this data again.

        if ( DELETE_SHUFFLE_FILES ) {
            
            // we have to close ALL of our output streams now.
            for( File shuffle : shuffles ) {
                shuffle.delete();
            }

        }

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

