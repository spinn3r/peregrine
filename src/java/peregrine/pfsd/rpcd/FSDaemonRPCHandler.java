package peregrine.pfsd.rpcd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import peregrine.util.*;
import peregrine.rpc.*;
import peregrine.rpcd.*;
import peregrine.rpcd.delegate.*;
import peregrine.controller.*;
import peregrine.controller.rpcd.*;
import peregrine.controller.rpcd.delegate.*;
import peregrine.pfsd.*;
import peregrine.pfsd.rpcd.delegate.*;

import com.spinn3r.log5j.*;

/**
 */
public class FSDaemonRPCHandler extends BaseRPCHandler<FSDaemon> {

	private static Map<String,RPCDelegate<FSDaemon>> handlers = new HashMap() {{
    	
    	put( "/shuffler/RPC",    new ShufflerRPCDelegate() );
        put( "/mapper/RPC",      new MapperRPCDelegate() );
        put( "/reducer/RPC",     new ReducerRPCDelegate() );
        put( "/merger/RPC",      new MergerRPCDelegate() );
        
    }};

    private FSDaemon daemon;

    private FSHandler handler;

    public FSDaemonRPCHandler( FSDaemon daemon, FSHandler handler, String uri ) {
    	
        super( daemon.getExecutorService( ControllerRPCHandler.class ), uri );
    	
        this.handler = handler;
        this.daemon = daemon;
        
    }
    
    @Override
    public RPCDelegate<FSDaemon> getRPCDelegate( String path ) {
    	return handlers.get( path );	
    }
 
    @Override
    public void handleMessage( RPCDelegate handler, Channel channel, Message message ) 
        throws Exception {
        	
       	handler.handleMessage(  daemon, channel, message );
        	
    }   
    
}
