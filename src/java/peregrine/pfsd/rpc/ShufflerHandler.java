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
public class ShufflerHandler extends RPCHandler {

    private static final Logger log = Logger.getLogger();

    public void handleMessage( FSDaemon daemon, Message message )
        throws Exception {

        String action = message.get( "action" );

        if ( "flush".equals( action ) ) {
            
            daemon.shufflerFactory.flush();
            return;

        }

        throw new Exception( String.format( "No handler for action %s with message %s", action, message ) );

    }
    
}
