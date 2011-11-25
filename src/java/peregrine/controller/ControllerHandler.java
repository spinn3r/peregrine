package peregrine.controller;

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
import peregrine.http.*;
import peregrine.util.netty.*;

import com.spinn3r.log5j.*;

/**
 */
public class ControllerHandler extends DefaultChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    protected Config config;
    protected ControllerDaemon controllerDaemon;
    
    public ControllerHandler( Config config,
                              ControllerDaemon controllerDaemon ) {

        this.config = config;
        this.controllerDaemon = controllerDaemon;

    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        super.messageReceived( ctx, e );
        
        // introspect requests and route them to the correct handler.  For
        // example, PUT request need to be handled separately than GET requests
        // as well as DELETE and HEAD.
        
        Object message = e.getMessage();

        if ( message instanceof HttpRequest ) {

            HttpMethod method = request.getMethod();
        	
            if ( method == POST ) {
                ControllerRPCHandler handler = new ControllerRPCHandler( config, controllerDaemon, request.getUri() );
                handler.messageReceived( ctx, e );
            }

        }

    }
    
}

