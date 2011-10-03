package peregrine.map;

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

public class MergeWithFullOuterJoinTask extends BaseMapperTask {
    
    private Merger merger;

    public Object call() throws Exception {

        merger = (Merger)super.newMapper();

        try {
            
            setup();
            merger.init( getJobOutput() );
            
            doCall();
            
        } finally {
            merger.cleanup();
            teardown();
        }

        return null;
        
    }

    private void doCall() throws Exception {

        System.out.printf( "Running merge jobs on host: %s\n", host );

        //FIXME: this task doesn't surface global_chunk_id so technically this
        //would not work with the distributed version.

        LocalPartitionReaderListener listener = new MapperChunkRolloverListener( this );
        
        // FIXME: move this to an input factory.
        List<LocalPartitionReader> readers = new ArrayList();

        for( InputReference ref : getInput().getReferences() ) {

            FileInputReference file = (FileInputReference) ref;
            
            readers.add( new LocalPartitionReader( partition, host, file.getPath(), listener ) );
            
        }

        LocalMerger localMerger = new LocalMerger( readers );

        while( true ) {

            JoinedTuple joined = localMerger.next();

            if ( joined == null )
                break;
            
            this.merger.map( joined.key, joined.values );
            
        }

    }
    
}