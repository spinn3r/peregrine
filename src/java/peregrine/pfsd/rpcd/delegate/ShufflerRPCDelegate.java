package peregrine.pfsd.rpcd.delegate;

import org.jboss.netty.channel.*;

import peregrine.pfsd.*;
import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;

/**
 */
public class ShufflerRPCDelegate extends RPCDelegate<FSDaemon> {

    public void handleMessage( FSDaemon daemon, Channel channel, Message message )
        throws Exception {
    	
        String action = message.get( "action" );

        if ( "flush".equals( action ) ) {
            daemon.shuffleReceiverFactory.flush();
            return;

        }

        throw new Exception( String.format( "No handler for action %s with message %s", action, message ) );

    }
    
}
