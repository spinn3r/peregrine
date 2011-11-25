package peregrine.pfsd.rpc;

import java.util.concurrent.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.util.*;
import peregrine.task.*;
import peregrine.pfsd.*;

import com.spinn3r.log5j.*;

/**
 */
public class ReducerRPCDelegate extends MapperRPCDelegate {

    private static final Logger log = Logger.getLogger();

    @Override
    protected void exec( FSDaemon daemon,
                         Class delegate,
                         Config config,
                         Partition partition,
                         Input input,
                         Output output )
        throws Exception {

        log.info( "Running %s with input %s and output %s", delegate.getName(), input, output );

        ShuffleInputReference shuffleInput = (ShuffleInputReference)input.getReferences().get( 0 );
        
        log.info( "Using shuffle input : %s ", shuffleInput.getName() );

        ReducerTask task = new ReducerTask( config, partition, delegate, shuffleInput );
        task.setInput( input );
        task.setOutput( output );

        daemon.getExecutorService( getClass() ).submit( task );

    }

}
