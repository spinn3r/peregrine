package peregrine.controller.rpcd;

import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.net.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.controller.rpcd.delegate.*;
import peregrine.http.*;
import peregrine.rpc.*;
import peregrine.rpcd.*;
import peregrine.rpcd.delegate.*;
import com.spinn3r.log5j.*;

/**
 */
public class ControllerRPCHandler extends BaseRPCHandler<ControllerDaemon> {

    private static final Logger log = Logger.getLogger();

    protected Config config;
    protected ControllerDaemon controllerDaemon;
    
    public ControllerRPCHandler( Config config,
                                 ControllerDaemon controllerDaemon,
                                 String uri ) {

        super( controllerDaemon.getExecutorService( ControllerRPCHandler.class ), uri );

        this.config = config;
        this.controllerDaemon = controllerDaemon;

    }

    @Override
    public RPCDelegate<ControllerDaemon> getRPCDelegate( String uri ) {
        return new ControllerRPCDelegate();
    }

    @Override
    public void handleMessage( RPCDelegate<ControllerDaemon> handler, Channel channel, Message message ) 
    	throws Exception {

    	handler.handleMessage( controllerDaemon, channel, message );
    	
    }

}

