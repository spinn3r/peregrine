package peregrine.pfsd;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.rpc.*;
import peregrine.shuffle.receiver.*;
import peregrine.task.*;
import peregrine.util.*;
import peregrine.util.netty.*;

import com.spinn3r.log5j.Logger;

public class FSDaemon extends BaseDaemon {

    private static final Logger log = Logger.getLogger();
    
    /**
     * Each daemon can only have one shuffle instance.
     */
    public ShuffleReceiverFactory shuffleReceiverFactory;

    private HeartbeatTimer heartbeatTimer;
    
    public FSDaemon( Config config ) {

        this.setConfig( config );

        init();

        shuffleReceiverFactory = new ShuffleReceiverFactory( config ); 

        if ( config.getController() != null )
            heartbeatTimer = new HeartbeatTimer( config );
        
    }

    @Override
    public ChannelPipelineFactory getChannelPipelineFactory() {
        return new FSPipelineFactory( getConfig(), this );
    }
    
    public void shutdown() {

        if ( heartbeatTimer != null )
            heartbeatTimer.cancel();

        super.shutdown();
        
    }

}
