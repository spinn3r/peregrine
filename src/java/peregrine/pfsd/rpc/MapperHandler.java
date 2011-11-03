package peregrine.pfsd.rpc;

import peregrine.pfsd.*;

import java.util.*;
import java.util.concurrent.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.util.*;

import com.spinn3r.log5j.*;

import peregrine.rpc.*;

import peregrine.io.*;
import peregrine.task.*;

/**
 */
public class MapperHandler extends RPCHandler {

    private static final Logger log = Logger.getLogger();

    private static ExecutorService executors =
        Executors.newCachedThreadPool( new DefaultThreadFactory( MapperHandler.class) );

    public void handleMessage( FSDaemon daemon, Message message )
        throws Exception {

        String action = message.get( "action" );

        if ( "exec".equals( action ) ) {

            log.info( "Going to map from action: %s", message );

            Input input            = readInput( message );
            Output output          = readOutput( message );
            Partition partition    = new Partition( message.getInt( "partition" ) );
            Class delegate         = Class.forName( message.get( "delegate" ) );
            Config config          = daemon.config;

            exec( delegate, config, partition, input, output );
            
            return;

        }

        throw new Exception( String.format( "No handler for action %s with message %s", action, message ) );

    }

    protected void exec( Class delegate, Config config, Partition partition, Input input, Output output )
        throws Exception {

        MapperTask task = new MapperTask();
        
        task.init( config, config.getMembership(), partition, config.getHost(), delegate );
        
        task.setInput( input );
        task.setOutput( output );

        log.info( "Running delegate %s with input %s and output %s", delegate.getName(), input, output );

        executors.submit( task );

    }
    
    protected Input readInput( Message message ) {

        Input input = new Input();

        for( String val : readList( message, "input." ) ) {
            
            String[] split = val.split( ":" );

            if ( split.length < 2 )
                throw new RuntimeException( "Unable to split arg: " + val );
            
            String type      = split[0];
            String arg       = split[1];

            if ( "broadcast".equals( type ) )
                input.add( new BroadcastInputReference( arg ) );

            if ( "file".equals( type ) )
                input.add( new FileInputReference( arg ) );

            if ( "shuffle".equals( type ) )
                input.add( new ShuffleInputReference( arg ) );

        }

        return input;
        
    }

    protected Output readOutput( Message message ) {

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

            if ( "shuffle".equals( type ) )
                output.add( new ShuffleOutputReference( arg ) );

        }

        return output;

    }

    protected List<String> readList( Message message, String prefix ) {

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
