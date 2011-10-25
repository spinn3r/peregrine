package peregrine.task;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;

import com.spinn3r.log5j.*;

public class MergerTask extends BaseMapperTask {

    private static final Logger log = Logger.getLogger();

    private Merger merger;

    public Object call() throws Exception {

        merger = (Merger)super.newMapper();

        try {
            
            setup();
            merger.setBroadcastInput( getBroadcastInput() );
            merger.init( getJobOutput() );

            try {
                doCall();
            } finally {
                merger.cleanup();
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

        log.info( "Running merge jobs on host: %s ...", host );

        listeners.add( new MergerLocalPartitionListener() );
        
        List<LocalPartitionReader> readers = getLocalPartitionReaders();

        LocalMerger localMerger = new LocalMerger( readers );

        while( true ) {

            JoinedTuple joined = localMerger.next();

            if ( joined == null )
                break;
            
            this.merger.map( joined.key, joined.values );
            
        }

        log.info( "Running merge jobs on host: %s ... done", host );

    }
    
}

/**
 * Used so that we can keep track of progress.
 */
class MergerLocalPartitionListener implements LocalPartitionReaderListener {

    private static final Logger log = Logger.getLogger();

    @Override
    public void onChunk( ChunkReference ref ) {
        log.info( "Merging chunk: %s" , ref );
    }

    @Override
    public void onChunkEnd( ChunkReference ref ) {}

}
