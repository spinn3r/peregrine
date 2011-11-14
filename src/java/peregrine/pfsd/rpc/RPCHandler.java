package peregrine.pfsd.rpc;

import org.jboss.netty.channel.*;

import peregrine.pfsd.*;
import peregrine.rpc.*;

/**
 */
public abstract class RPCHandler {

    public abstract void handleMessage( Channel channel, FSDaemon daemon, Message message )
        throws Exception;

}
