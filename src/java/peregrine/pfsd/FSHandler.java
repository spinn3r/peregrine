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
    
    public static int BUFFER_SIZE = 16384;
    
    public static byte[] EOF = new byte[0];

    private HttpRequest request = null;

    private HttpMethod method = null;

    private String path = null;

    private OutputStream asyncOutputStream = null;

    /**
     * NR of bytes written.
     */
    private long written = 0;
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        Object message = e.getMessage();

        if ( message instanceof HttpRequest ) {

            this.request = (HttpRequest)message;
            this.method = request.getMethod();
            
            //TODO handle other methods other than GET here
            if ( method != PUT )  {
                sendError(ctx, METHOD_NOT_ALLOWED);
                return;
            }
          
            path = request.getUri();
            path = sanitizeUri( path );
            
            if ( path == null || ! path.startsWith( "/" ) ) {
                sendError(ctx, FORBIDDEN);
                return;
            }

            log.info( "%s: %s", method, request.getUri() );

            if ( method == PUT ) {
                asyncOutputStream = new AsyncOutputStream( path );
                asyncOutputStream = new BufferedOutputStream( asyncOutputStream, BUFFER_SIZE );
                return;
            }

            if ( method == GET ) {

                //handle HTTP GETs here.
                
            }

        } else if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                ChannelBuffer content = chunk.getContent();
                byte[] data = content.array();

                written += data.length;
                
                asyncOutputStream.write( data );

            } else {

                asyncOutputStream.write( EOF );
                asyncOutputStream.close();

                // log that this was written successfully including the NR of
                // bytes.

                log.info( "Wrote %,d bytes to %s", written, path );
                
                HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

                Channel ch = e.getChannel();

                ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);

            }

        }
            
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
        return Config.PFS_ROOT + new URI( uri ).getPath();
        
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

