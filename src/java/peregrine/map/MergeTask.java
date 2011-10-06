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
import peregrine.io.partition.*;

public class MergeTask extends BaseMapperTask {
    
    private Merger merger;

    public Object call() throws Exception {

        merger = (Merger)super.newMapper();

        try {
            
            setup();
            merger.setBroadcastInput( getBroadcastInput() );
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

        LocalPartitionReaderListener listener = new MapperChunkRolloverListener( this );
        
        List<LocalPartitionReader> readers = getLocalPartitionReaders( listener );

        LocalMerger localMerger = new LocalMerger( readers );

        while( true ) {

            JoinedTuple joined = localMerger.next();

            if ( joined == null )
                break;
            
            this.merger.map( joined.key, joined.values );
            
        }

    }
    
}