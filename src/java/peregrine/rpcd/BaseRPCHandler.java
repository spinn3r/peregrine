package peregrine.rpcd;

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
import peregrine.rpcd.delegate.*;
import com.spinn3r.log5j.*;

/**
 */
public abstract class BaseRPCHandler<T> extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    private Channel channel;

    private Message message;

    private ExecutorService executorService;

    private String uri;
    
    public BaseRPCHandler( ExecutorService executorService, String uri ) {
        this.executorService = executorService;
        this.uri = uri;
    }

    public abstract void handleMessage( RPCDelegate<T> handler, Channel channel, Message message ) 
    	throws Exception;
    
    public abstract RPCDelegate<T> getRPCDelegate( String path );

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        Object message = e.getMessage();

        channel = e.getChannel();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                System.out.printf( "FIXME1\n" );
                
                ChannelBuffer content = chunk.getContent();
                byte[] data = new byte[ content.writerIndex() ];
                content.readBytes( data );
                
                this.message = new Message( new String( data ) );
                
            } else {

                System.out.printf( "FIXME2\n" );

                doHandleMessage();

            }
                
        }
        
    }

    private void doHandleMessage() {

        try {
            
            URI uri = new URI( this.uri );

            String path = uri.getPath();

            final RPCDelegate delegate = getRPCDelegate( path );

            if ( delegate != null ) {
            	
            	log.info( "Handling message %s for URI: %s with %s", message, uri, delegate );
            	
                executorService.submit( new AsyncMessageHandler( channel, message ) {

                        public void doAction() throws Exception {
                            handleMessage( delegate, channel, message );
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

    /**
     * Perform an action in a background thread, and then send a response code.
     */
    abstract class AsyncMessageHandler implements Runnable {

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
    
}
