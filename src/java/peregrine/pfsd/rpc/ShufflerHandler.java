package peregrine.pfsd.rpc;

import peregrine.pfsd.*;

import peregrine.rpc.*;

/**
 */
public class ShufflerHandler extends RPCHandler {

    public void handleMessage( FSDaemon daemon, Message message )
        throws Exception {

        String action = message.get( "action" );

        if ( "flush".equals( action ) ) {

            //FIXME: this should probably be async... 
            daemon.shuffleReceiverFactory.flush();
            return;

        }

        throw new Exception( String.format( "No handler for action %s with message %s", action, message ) );

    }
    
}
