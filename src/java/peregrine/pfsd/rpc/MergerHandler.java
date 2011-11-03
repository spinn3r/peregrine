package peregrine.pfsd.rpc;

import java.util.concurrent.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.util.*;
import peregrine.task.*;

import com.spinn3r.log5j.*;

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

        MergerTask task = new MergerTask();

        task.init( config, config.getMembership(), partition, config.getHost(), delegate );

        task.setInput( input );
        task.setOutput( output );
        
        executors.submit( task );

    }

}
