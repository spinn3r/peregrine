package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.*;
import org.jboss.netty.handler.stream.*;
import org.jboss.netty.util.*;

import peregrine.*;
import peregrine.io.async.*;
import peregrine.io.partition.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 */
public class FSPostDirectHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    private static ExecutorService executors =
        Executors.newCachedThreadPool( new DefaultThreadFactory( FSPostDirectHandler.class) );
    
    private FSHandler handler;

    private Channel channel;

    private Map<String,List<String>> message;
    
    public FSPostDirectHandler( FSHandler handler ) {
        this.handler = handler;
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        Object message = e.getMessage();

        channel = e.getChannel();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                ChannelBuffer content = chunk.getContent();
                byte[] data = content.array();
                
                this.message = new QueryStringDecoder( new String( data ) ).getParameters();

            } else {

                handleMessage();

            }
                
        }
        
    }

    private void handleMessage() {

        String action = message.get( "action" ).get( 0 );

        if ( "flush".equals( action ) ) {

            // we don't need to wait until this stops.
            executors.submit( new AsyncAction( channel, message ) {

                    public void doAction() throws Exception {

                        handler.daemon.shufflerFactory.flush();

                    }
                    
                } );

            return;
            
        }

        log.warn( "No handler for action %s with message %s", action, message );
        
        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, INTERNAL_SERVER_ERROR );
        channel.write(response).addListener(ChannelFutureListener.CLOSE);
        return;

    }
    
}

abstract class AsyncAction implements Runnable {

    private static final Logger log = Logger.getLogger();

    private Channel channel;
    private Map<String,List<String>> message;
    
    public AsyncAction( Channel channel,
                        Map<String,List<String>> message ) {
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