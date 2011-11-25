package peregrine.rpcd.delegate;

import org.jboss.netty.channel.*;

import peregrine.rpc.*;

/**
 */
public abstract class RPCDelegate<T> {

    /**
     * @param parent The parent object invoking this request. Usually an
     * FSDaemon or a Controller.
     */
    public abstract void handleMessage( T parent,
                                        Channel channel,
                                        Message message )
        throws Exception;

}
