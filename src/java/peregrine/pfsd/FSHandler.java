package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.net.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.*;
import org.jboss.netty.handler.stream.*;
import org.jboss.netty.util.*;

import peregrine.*;
import peregrine.io.async.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 */
public class FSHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();
    
    private HttpRequest request = null;

    protected String path = null;

    /**
     * Root directory for serving files.
     */
    private String root;

    private SimpleChannelUpstreamHandler upstream = null;

    public FSHandler( String root ) {
        this.root = root;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        // introspect requests and route them to the correct handler.  For
        // example, PUT request need to be handled separately than GET requests
        // as well as DELETE and HEAD.
        
        Object message = e.getMessage();

        if ( message instanceof HttpRequest ) {

            this.request = (HttpRequest)message;

            HttpMethod method = request.getMethod();

            // log EVERY request no matter the source.
            log.info( "%s: %s", method, request.getUri() );

            path = request.getUri();
            path = sanitizeUri( path );
            
            if ( path == null || ! path.startsWith( "/" ) ) {
                sendError(ctx, FORBIDDEN);
                return;
            }

            if ( method == PUT ) {
                upstream = new FSPutDirectHandler( this );
            }

            if ( method == DELETE ) {
                upstream = new FSDeleteDirectHandler( this );
            }

            if ( method == GET ) {
                // TODO
            }

            //TODO handle other methods other than GET here
            if ( upstream == null )  {
                sendError(ctx, METHOD_NOT_ALLOWED);
                return;
            }

        }

        if ( upstream != null )
            upstream.messageReceived( ctx, e );

    }

    private String sanitizeUri(String uri) throws java.net.URISyntaxException {

        // TODO: I believe this is actually wrong and that we have to try
        // ISO-8601 first according to the HTTP spec but I need to research this
        // problem.

        try {

            uri = URLDecoder.decode(uri, "UTF-8");

        } catch (UnsupportedEncodingException e) {

            try {
                uri = URLDecoder.decode(uri, "ISO-8859-1");
            } catch (UnsupportedEncodingException e1) {
                throw new Error();
            }

        }

        if ( uri.contains( "../" ) || uri.contains( "/.." ) )
            return null;

        // Convert to absolute path.
        return root + new URI( uri ).getPath();
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
        
        Channel ch = e.getChannel();
        Throwable cause = e.getCause();

        log.error( "Could not handle request: " , cause );

        if (cause instanceof TooLongFrameException) {
            sendError(ctx, BAD_REQUEST);
            return;
        }

        if (ch.isConnected()) {
            sendError(ctx, INTERNAL_SERVER_ERROR);
        }
        
    }

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);

        response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

        String msg = "Failure: " + status.toString() + "\r\n";

        response.setContent( ChannelBuffers.copiedBuffer( msg, CharsetUtil.UTF_8));

        // Close the connection as soon as the error message is sent.
        ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);

    }

}

