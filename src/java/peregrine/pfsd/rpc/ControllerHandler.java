package peregrine.pfsd.rpc;

import peregrine.pfsd.*;

import peregrine.config.Host;
import peregrine.config.Partition;
import peregrine.rpc.*;

/**
 */
public class ControllerHandler extends RPCHandler {

    public void handleMessage( FSDaemon daemon, Message message )
        throws Exception {

        String action = message.get( "action" );

        if ( "complete".equals( action ) ) {

            Host host       = Host.parse( message.get( "host" ) );
            Partition part  = new Partition( message.getInt( "partition" ) );

            daemon.getScheduler().markComplete( host, part );

            return;

        }

        if ( "failed".equals( action ) ) {

            Host host          = Host.parse( message.get( "host" ) );
            Partition part     = new Partition( message.getInt( "partition" ) );
            String stacktrace  = message.get( "stacktrace" );

            daemon.getScheduler().markFailed( host, part, stacktrace );
            
            return;

        }

        if ( "progress".equals( action ) ) {

            /*
            Host host       = Host.parse( message.get( "host" ) );
            Partition part  = new Partition( message.getInt( "partition" ) );
            String cause    = message.get( "cause" );

            log.error( "Host %s has failed with cause '%s'", host, cause );
            */
            
            return;

        }

        throw new Exception( String.format( "No handler for action %s with message %s", action, message ) );

    }
    
}
