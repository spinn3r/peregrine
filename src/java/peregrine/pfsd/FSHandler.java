package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.net.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.config.Config;
import peregrine.http.*;
import peregrine.util.netty.*;

import com.spinn3r.log5j.*;

/**
 */
public class FSHandler extends DefaultChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    public static final String X_PIPELINE_HEADER = "X-pipeline";
    
    protected String path = null;

    protected SimpleChannelUpstreamHandler upstream = null;

    /**
     * True when we should pipeline this DELETE/PUT request to another host.
     */
    protected boolean pipeline = false;

    protected HttpClient remote = null;

    protected Config config;

    protected FSDaemon daemon;
    
    public FSHandler( Config config,
                      FSDaemon daemon ) {

        this.daemon = daemon;
        this.config = config;

    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

    	super.messageReceived( ctx, e );
    	
        // introspect requests and route them to the correct handler.  For
        // example, PUT request need to be handled separately than GET requests
        // as well as DELETE and HEAD.
        
        Object message = e.getMessage();

        if ( message instanceof HttpRequest ) {

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
                    upstream = new FSPutShuffleHandler( daemon, this );
                } else {
                    upstream = new FSPutDirectHandler( daemon, this );
                }

            }

            if ( method == DELETE ) {
                upstream = new FSDeleteDirectHandler( daemon, this );
            }

            if ( method == HEAD ) {
                upstream = new FSHeadDirectHandler( daemon, this );
            }

            if ( method == POST ) {
                upstream = new FSPostDirectHandler( daemon, this );
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

        if ( pipeline ) {
            handlePipeline( ctx, message );
        }

        if ( upstream != null )
            upstream.messageReceived( ctx, e );

    }
    
    private void handlePipeline( final ChannelHandlerContext ctx , Object message ) throws IOException {

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
                
                remote = new HttpClient( request, uri ) {

                        @Override
                        public void onCapacityChange( boolean hasCapacity ) {

                            // it is important that we change the readable
                            // status of this thread so that we don't block on
                            // pipelined requests of we would simply lock
                            // everything up..
                            
                            ctx.getChannel().setReadable( hasCapacity );
                            
                        }

                        @Override
                        public void onClose( boolean success, Throwable cause ) {
                            
                            HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
                            ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);

                        }
                        
                    };
                
            } else if ( remote != null && message instanceof HttpChunk ) {

                HttpChunk chunk = (HttpChunk)message;

                if ( ! chunk.isLast() ) {
                    
                    ChannelBuffer content = chunk.getContent();
                    remote.write( content );

                } else {

                    // Any HTTP exceptions on the remote host should bubble up
                    // in close() and be passed on to callers.... 
                    
                    remote.close( false );
                    
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
    
}

