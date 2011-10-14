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

                System.out.printf( "data: %s\n", new String( data ) );
                
                this.message = new QueryStringDecoder( new String( data ) ).getParameters();

            } else {

                handleMessage();

            }
                
        }
        
    }

    private void handleMessage() {

        String action = message.get( "action" ).get( 0 );

        try {
        
            if ( "flush".equals( action ) ) {

                log.info( "Flushing shufflers..." );
                
                handler.daemon.shufflerFactory.flush();

                log.info( "Flushing shufflers...done" );

                HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
                channel.write(response).addListener(ChannelFutureListener.CLOSE);

                return;
                
            }
                
        } catch ( IOException e ) {
            log.error( "Unable handle message %s: ", message, e );
        }

        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, INTERNAL_SERVER_ERROR );
        channel.write(response).addListener(ChannelFutureListener.CLOSE);
        return;

    }
    
}
