package peregrine.pfsd.rpc;

import org.jboss.netty.channel.*;

import peregrine.pfsd.*;

import peregrine.rpc.*;

/**
 */
public class ShufflerHandler extends RPCHandler<FSDaemon> {

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
