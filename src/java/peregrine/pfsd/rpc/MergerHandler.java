package peregrine.pfsd;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.io.async.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.shuffle.*;

import com.spinn3r.log5j.*;

import peregrine.rpc.*;

/**
 */
public class MergerHandler extends MapperHandler {

    private static final Logger log = Logger.getLogger();

    private static ExecutorService executors =
        Executors.newCachedThreadPool( new DefaultThreadFactory( MergerHandler.class) );

    @Override
    protected void exec( Class delegate, Config config, Partition partition, Input input, Output output )
        throws Exception {

        log.info( "Running %s with input %s and output %s", delegate.getName(), input, output );

        MergeTask task = new MergeTask();

        task.init( config, config.getPartitionMembership(), partition, config.getHost(), delegate );

        task.setInput( input );
        task.setOutput( output );
        
        executors.submit( task );

    }

}
