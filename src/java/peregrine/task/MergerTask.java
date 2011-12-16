package peregrine.task;

import java.util.*;
import peregrine.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.sysstat.*;

import com.spinn3r.log5j.*;

public class MergerTask extends BaseMapperTask {

    private static final Logger log = Logger.getLogger();

    private Merger merger;

    public Object call() throws Exception {

        merger = (Merger)super.newMapper();

        SystemProfiler profiler = config.getSystemProfiler();

        try {

            log.info( "Running %s on %s", delegate, partition );
            
            setup();
            merger.setBroadcastInput( getBroadcastInput() );
            merger.init( getJobOutput() );

            try {
                doCall();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

            try {
                merger.cleanup();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

            try {
                teardown();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

            setStatus( TaskStatus.COMPLETE );

        } catch ( Throwable t ) { 
            handleFailure( log, t );
        } finally {
            report();
            log.info( "Ran with profiler rate: \n%s", profiler.rate() );
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
            
            this.merger.merge( joined.key, joined.values );
            
        }

        log.info( "Running merge jobs on host: %s ... done", host );

    }
    
}

/**
 * Used so that we can keep track of progress as we execute jobs. Multiple
 * chunks will be used to we need to keep track of which ones we're running
 * over.
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
