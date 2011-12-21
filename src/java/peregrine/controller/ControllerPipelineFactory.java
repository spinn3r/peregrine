package peregrine.controller;

import static org.jboss.netty.channel.Channels.*;
import static peregrine.pfsd.FSPipelineFactory.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.pfsd.*;

/**
 * Netty pipeline handler for our controller.
 *
 */
public class ControllerPipelineFactory implements ChannelPipelineFactory {

    private Config config;
    private ControllerDaemon controllerDaemon;
    
    public ControllerPipelineFactory( ControllerDaemon controllerDaemon, 
    							      Config config ) {

        this.controllerDaemon = controllerDaemon;
    	this.config = config;
    	
    }
    
    public ChannelPipeline getPipeline() throws Exception {

        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

        //pipeline.addLast("hex",            new HexPipelineEncoder());
        pipeline.addLast("decoder",        new HttpRequestDecoder( MAX_INITIAL_LINE_LENGTH ,
                                                                   MAX_HEADER_SIZE,
                                                                   MAX_CHUNK_SIZE ) );

        pipeline.addLast("encoder",        new HttpResponseEncoder() );
        pipeline.addLast("handler",        new ControllerHandler( config, controllerDaemon ) );
        
        return pipeline;

    }

}
