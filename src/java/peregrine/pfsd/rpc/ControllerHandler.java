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

/**
 */
public class ControllerHandler extends RPCHandler {

    private static final Logger log = Logger.getLogger();

    public void handleMessage( FSDaemon daemon, Map<String,List<String>> message )
        throws Exception {

        String action = message.get( "action" ).get( 0 );

        if ( "map_complete".equals( action ) ) {
            
            return;

        }

        throw new Exception( String.format( "No handler for action %s with message %s", action, message ) );

    }
    
}
