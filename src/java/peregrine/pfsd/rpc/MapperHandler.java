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

import com.spinn3r.log5j.*;

import peregrine.rpc.*;

/**
 */
public class MapperHandler extends RPCHandler {

    private static final Logger log = Logger.getLogger();

    private static ExecutorService executors =
        Executors.newCachedThreadPool( new DefaultThreadFactory( MapperHandler.class) );

    public void handleMessage( FSDaemon daemon, Message message )
        throws Exception {

        String action = message.get( "action" );

        if ( "map".equals( action ) ) {

            log.info( "Going to map from action: %s", message );

            Input input            = readInput( message );
            Output output          = readOutput( message );
            Partition partition    = new Partition( Integer.parseInt( message.get( "partition" ) ) );
            Class mapper           = Class.forName( message.get( "mapper" ) );

            MapperTask task = new MapperTask();

            Config config = daemon.config;
            
            task.init( config, config.getPartitionMembership(), partition, config.getHost(), mapper );

            task.setInput( input );
            task.setOutput( output );

            log.info( "Running mapper %s with input %s and output %s", mapper.getName(), input, output );

            executors.submit( task );
            
            return;

        }

        throw new Exception( String.format( "No handler for action %s with message %s", action, message ) );

    }

    private Input readInput( Message message ) {

        Input input = new Input();

        for( String val : readList( message, "input." ) ) {
            
            String[] split = val.split( ":" );

            String type      = split[0];
            String arg       = split[1];

            if ( "broadcast".equals( type ) )
                input.add( new BroadcastInputReference( arg ) );

            if ( "file".equals( type ) )
                input.add( new FileInputReference( arg ) );

        }

        return input;
        
    }

    private Output readOutput( Message message ) {

        Output output = new Output();

        for( String val : readList( message, "output." ) ) {
            
            String[] split = val.split( ":" );

            String type      = split[0];
            String arg       = split[1];

            if ( "broadcast".equals( type ) )
                output.add( new BroadcastOutputReference( arg ) );

            if ( "file".equals( type ) ) {
                boolean append = split[2].equals( "true" );
                output.add( new FileOutputReference( arg, append ) );
            }

        }

        return output;

    }

    private List<String> readList( Message message, String prefix ) {

        List<String> result = new ArrayList();
    
        for( int i = 0 ; i < Integer.MAX_VALUE; ++i ) {

            String val = message.get( prefix + i );

            if ( val == null )
                break;

            result.add( val );
            
        }

        return result;
        
    }
    
}
