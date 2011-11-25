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
public class MergerRPCDelegate extends MapperRPCDelegate {

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

        MergerTask task = new MergerTask();

        task.init( config, config.getMembership(), partition, config.getHost(), delegate );

        task.setInput( input );
        task.setOutput( output );
        
        daemon.getExecutorService( getClass() ).submit( task );

    }

}
