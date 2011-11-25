package peregrine.util.netty;

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

import com.spinn3r.log5j.*;

/**
 */
public class DefaultChannelUpstreamHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    public HttpRequest request;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        Object message = e.getMessage();

        if ( message instanceof HttpRequest ) {
            this.request = (HttpRequest)message;
        }

    }
            
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {

        Channel ch = e.getChannel();
        Throwable cause = e.getCause();

        String tag = request.getHeader( HttpClient.X_TAG );
        
        if ( request == null )
            log.error( String.format( "Could not handle initial request (tag=%s): ", tag ), cause );
        else
            log.error( String.format( "Could not handle request (tag=%s): %s %s", tag, request.getMethod(), request.getUri() ) , cause );

        if (cause instanceof TooLongFrameException) {
            sendError(ctx, BAD_REQUEST);
            return;
        }

        if (ch.isConnected()) {
            sendError(ctx, INTERNAL_SERVER_ERROR);
        }
        
    }

    protected void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);

        /*
        response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

        String msg = "Failure: " + status.toString() + "\r\n";

        response.setContent( ChannelBuffers.copiedBuffer( msg, CharsetUtil.UTF_8));
        */
        
        // Close the connection as soon as the error message is sent.
        ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);

    }
    
}

