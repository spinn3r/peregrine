package peregrine.pfsd.rpc;


import peregrine.pfsd.*;
import peregrine.rpc.*;

/**
 */
public abstract class RPCHandler {

    public abstract void handleMessage( FSDaemon daemon, Message message )
        throws Exception;
    
    
}
