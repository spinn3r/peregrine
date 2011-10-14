package peregrine.pfs;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.util.*;
import peregrine.io.async.*;

import static peregrine.pfsd.FSPipelineFactory.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.channel.Channels.*;

import com.spinn3r.log5j.Logger;

/**
 * Simple HTTP client which will block until the request is available.  This is
 * used for the RPC protocol and other places where synchoronous writes are
 * needed by the framework.
 * 
 */
public class HttpClient {

    DefaultHttpRequest request = null;

    BlockingQueue queue = new LinkedBlockingQueue(1);

    URI uri;

    String host;
    
    int port;

    Channel channel;
    
    public HttpClient( URI uri ) {
        this( uri, HttpMethod.GET );
    }
    
    public HttpClient( URI uri , HttpMethod method ) {

        // Prepare the HTTP request.
        this.request = new DefaultHttpRequest( HttpVersion.HTTP_1_1, method, uri.toASCIIString() );
        this.uri = uri;
        this.host = uri.getHost();
        this.port = uri.getPort();

        request.setHeader( HttpHeaders.Names.USER_AGENT, getClass().getName() );
        request.setHeader( HttpHeaders.Names.HOST, host );

    }

    public void setHeader( String name, String value ) {
        request.setHeader( name, value );
    }
    
    public void connect() throws IOException {

        ClientBootstrap bootstrap = new ClientBootstrap( RemoteChunkWriterClient.socketChannelFactory );

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new HttpClientPipelineFactory( this ) );

        // Start the connection attempt... where is the connect timeout set?
        ChannelFuture connectFuture = bootstrap.connect( new InetSocketAddress( host, port ) );

        connectFuture.awaitUninterruptibly();

        channel = connectFuture.getChannel();

        channel.write( request ).awaitUninterruptibly();
        
    }
    public void write( String message ) throws IOException {
        write( ChannelBuffers.wrappedBuffer( message.getBytes() ) );
    }

    public void write( ChannelBuffer message ) throws IOException {
        channel.write( message ).awaitUninterruptibly();
    }

    public void close() throws IOException {

        try {
        
            Object result = queue.take();
            
            if ( result instanceof IOException )
                throw (IOException)result;

        } catch ( InterruptedException e ) {
            throw new IOException( e );
        }
        
    }
    
}

class HttpClientPipelineFactory implements ChannelPipelineFactory {

    private HttpClient client;
    
    public HttpClientPipelineFactory( HttpClient client ) {
        this.client = client;
    }
    
    public ChannelPipeline getPipeline() throws Exception {

        ChannelPipeline pipeline = pipeline();

        pipeline.addLast( "codec",   new HttpClientCodec( MAX_INITIAL_LINE_LENGTH ,
                                                          MAX_HEADER_SIZE,
                                                          MAX_CHUNK_SIZE ));
        
        pipeline.addLast( "handler", new HttpClientHandler( client ));

        return pipeline;

    }
    
}

class HttpClientHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    private HttpClient client = null;
    
    public HttpClientHandler( HttpClient client ) {
        this.client = client;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        HttpResponse response = (HttpResponse) e.getMessage();

        log.info( "Received HTTP response: %s for %s", response.getStatus(), client.uri );

        if ( response.getStatus().getCode() != OK.getCode() ) {

            client.queue.put( new IOException( response.getStatus().toString() ) );
            return;

        }

        client.queue.put( Boolean.TRUE );
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {

        client.queue.put( e.getCause() );

    }

}

