package peregrine.pfsd;

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
import peregrine.pfsd.rpc.*;

import com.spinn3r.log5j.*;

/**
 */
public class FSPostDirectHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

	private static Map<String,RPCHandler<FSHandler>> handlers = new HashMap() {{
    	
    	put( "/shuffler/RPC",    new ShufflerHandler() );
        put( "/controller/RPC",  new ControllerHandler() );
        put( "/mapper/RPC",      new MapperHandler() );
        put( "/reducer/RPC",     new ReducerHandler() );
        put( "/merger/RPC",      new MergerHandler() );
        
    }};

    private FSDaemon daemon;

    private FSHandler handler;

    private Channel channel;

    private Message message;

    public FSPostDirectHandler( FSDaemon daemon, FSHandler handler ) {
        this.handler = handler;
        this.daemon = daemon;
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        Object message = e.getMessage();

        channel = e.getChannel();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                ChannelBuffer content = chunk.getContent();
                byte[] data = new byte[ content.writerIndex() ];
                content.readBytes( data );
                
                this.message = new Message( new String( data ) );
                
            } else {

                doHandleMessage();

            }
                
        }
        
    }

    public void handleMessage( RPCHandler handler, Channel channel, Message message ) 
    	throws Exception {
    	
    	handler.handleMessage(  daemon, channel, message );
    	
    }
    
    public RPCHandler getRPCHandler( String path ) {
    	return handlers.get( path );	
    }
    
    private void doHandleMessage() {

        try {
            
            URI uri = new URI( handler.request.getUri() );

            String path = uri.getPath();

            final RPCHandler rpcHandler = getRPCHandler( path );

            if ( rpcHandler != null ) {
            	
            	log.info( "Handling message %s for URI: %s with %s", message, uri, rpcHandler );
            	
                daemon.getExecutorService( getClass() ).submit( new AsyncMessageHandler( channel, message ) {

                        public void doAction() throws Exception {
                            handleMessage( rpcHandler, channel, message );
                        }
                        
                    } );

                return;
                
            } else {
                log.warn( "No handler for message %s at URI %s", message, uri );
            }

        } catch ( Exception e ) {
            log.error( "Could not handle RPC call: " , e );
        }

        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, INTERNAL_SERVER_ERROR );
        channel.write(response).addListener(ChannelFutureListener.CLOSE);
        return;

    }
    
}

/**
 * Perform an action in a background thread, and then send a response code.
 */
abstract class AsyncMessageHandler implements Runnable {

    private static final Logger log = Logger.getLogger();

    private Channel channel;
    private Message message;
    
    public AsyncMessageHandler( Channel channel,
                                Message message ) {
        this.channel = channel;
        this.message = message;
    }

    @Override
    public void run() {

        try {

            doAction();

            HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
            channel.write(response).addListener(ChannelFutureListener.CLOSE);

        } catch ( Exception e ) {
            
            log.error( "Unable handle message: " + message, e );

            HttpResponse response = new DefaultHttpResponse( HTTP_1_1, INTERNAL_SERVER_ERROR );
            channel.write(response).addListener(ChannelFutureListener.CLOSE);

        }

    }

    public abstract void doAction() throws Exception;

}