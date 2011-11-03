package peregrine.pfsd.rpc;


import peregrine.pfsd.*;
import peregrine.rpc.*;

import com.spinn3r.log5j.*;

/**
 */
public abstract class RPCHandler {

    private static final Logger log = Logger.getLogger();

    public abstract void handleMessage( FSDaemon daemon, Message message )
        throws Exception;
    
    
}
