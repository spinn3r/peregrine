package peregrine.pfsd;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.io.async.*;
import peregrine.io.partition.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

import peregrine.rpc.*;

/**
 */
public class ControllerHandler extends RPCHandler {

    private static final Logger log = Logger.getLogger();

    public void handleMessage( FSDaemon daemon, Message message )
        throws Exception {

        String action = message.get( "action" );

        if ( "map_complete".equals( action ) ) {

            Host host    = Host.parse( message.get( "host" ) );
            int part     = message.getInt( "partition" );
            Partition partition = new Partition( part );

            log.info( "Marking partition %s complete from host: %s", partition, host );
            
            daemon.getScheduler().markComplete( host, partition );

            return;

        }

        throw new Exception( String.format( "No handler for action %s with message %s", action, message ) );

    }
    
}
