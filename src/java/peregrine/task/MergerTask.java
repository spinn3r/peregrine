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
            
            doCall();

            setStatus( TaskStatus.COMPLETE );

        } catch ( Throwable t ) { 

            log.error( "Task failed: ", t );

            setStatus( TaskStatus.FAILED );
            setCause( t );
            
        } finally {
            merger.cleanup();
            teardown();
        }

        return null;

    }

    private void doCall() throws Exception {

        log.info( "Running merge jobs on host: %s ...", host );
        
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