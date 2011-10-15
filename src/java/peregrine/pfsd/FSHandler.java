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
import peregrine.pfs.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 */
public class FSHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    public static final String X_PIPELINE_HEADER = "X-pipeline";
    
    protected HttpRequest request = null;

    protected String path = null;

    protected SimpleChannelUpstreamHandler upstream = null;

    /**
     * True when we should pipeline this DELETE/PUT request to another host.
     */
    protected boolean pipeline = false;

    protected RemoteChunkWriterClient remote = null;

    protected Config config;

    protected FSDaemon daemon;
    
    public FSHandler( Config config,
                      FSDaemon daemon ) {

        this.daemon = daemon;
        this.config = config;

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

                URI uri = new URI( request.getUri() );

                if ( uri.getPath().contains( "/shuffle/" ) ) {
                    upstream = new FSPutShuffleHandler( this );
                } else {
                    upstream = new FSPutDirectHandler( this );
                }

            }

            if ( method == DELETE ) {
                upstream = new FSDeleteDirectHandler( this );
            }

            if ( method == HEAD ) {
                upstream = new FSHeadDirectHandler( this );
            }

            if ( method == POST ) {
                upstream = new FSPostDirectHandler( this );
            }
            
            if ( method == GET ) {
                // TODO
            }

            // this method needs pipelining... 
            if ( method == DELETE || method == PUT ) {
                pipeline = true;
            }
            
            //TODO handle other methods other than GET here
            if ( upstream == null )  {
                sendError(ctx, METHOD_NOT_ALLOWED);
                return;
            }

        }

        if ( upstream != null )
            upstream.messageReceived( ctx, e );

        if ( pipeline ) {
            handlePipeline( message );
        }
        
    }
    
    private void handlePipeline( Object message ) throws IOException {

        try {
            
            URI uri = new URI( request.getUri() );

            if ( message instanceof HttpRequest && remote == null ) {

                String x_pipeline = request.getHeader( X_PIPELINE_HEADER );

                if ( x_pipeline == null )
                    return;

                x_pipeline = x_pipeline.trim();

                if ( "".equals( x_pipeline ) )
                    return;

                log.info( "%s=%s", X_PIPELINE_HEADER, x_pipeline );

                String[] hosts = x_pipeline.split( " " );

                if ( hosts.length == 0 )
                    return;

                String host = hosts[0];

                x_pipeline = "";
                    
                for( int i = 1; i < hosts.length; ++i ) {
                    x_pipeline += hosts[i] + " ";
                }
                
                x_pipeline.trim();

                if ( ! "".equals( x_pipeline ) )
                    request.setHeader( X_PIPELINE_HEADER, x_pipeline );

                uri = new URI( String.format( "http://%s%s", host, uri.getPath() ) );

                log.info( "Going to pipeline requests to: %s ", uri );
                
                remote = new RemoteChunkWriterClient( request, uri );
                
            } else if ( remote != null && message instanceof HttpChunk ) {

                HttpChunk chunk = (HttpChunk)message;

                if ( ! chunk.isLast() ) {
                    
                    ChannelBuffer content = chunk.getContent();
                    
                    remote.write( content );

                } else {

                    // any HTTP exceptions on the remote host should bubble up
                    // in close() and be passed on to callers.
                    remote.close();
                    
                }
                
            }
            
        } catch ( URISyntaxException e ) {
            throw new IOException( "Invalid URI: ", e );
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
        return config.getRoot() + new URI( uri ).getPath();
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {

        Channel ch = e.getChannel();
        Throwable cause = e.getCause();

        String tag = request.getHeader( RemoteChunkWriterClient.X_TAG );
        
        if ( request == null )
            log.error( String.format( "Could not handle initial request (tag=%s): ", tag ), cause );
        else
            log.error( String.format( "Could not handle request (tag=%s): %s", tag, request.getUri() ) , cause );

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

        /*
        response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

        String msg = "Failure: " + status.toString() + "\r\n";

        response.setContent( ChannelBuffers.copiedBuffer( msg, CharsetUtil.UTF_8));
        */
        
        // Close the connection as soon as the error message is sent.
        ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);

    }

}

