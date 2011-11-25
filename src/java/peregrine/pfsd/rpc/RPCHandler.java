package peregrine.pfsd.rpc;

import org.jboss.netty.channel.*;

import peregrine.pfsd.*;
import peregrine.rpc.*;

/**
 */
public abstract class RPCHandler<T> {

    /**
     * @param parent The parent object invoking this request. Usually an
     * FSDaemon or a Controller.
     */
    public abstract void handleMessage( T parent,
                                        Channel channel,
                                        Message message )
        throws Exception;

}
