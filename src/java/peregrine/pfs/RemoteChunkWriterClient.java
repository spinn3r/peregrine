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

import peregrine.*;
import peregrine.io.async.*;
import peregrine.pfsd.*;
import peregrine.util.*;

/**
 * HTTP client that supports chunked PUT to a remote PFS node.
 * 
 */
public class RemoteChunkWriterClient extends BaseOutputStream {

    private static NioClientSocketChannelFactory socketChannelFactory =
        new NioClientSocketChannelFactory( Executors.newCachedThreadPool( new DefaultThreadFactory( RemoteChunkWriterClient.class ) ), 
                                           Executors.newCachedThreadPool( new DefaultThreadFactory( RemoteChunkWriterClient.class ) ) );

    public static final int PENDING  = -1;
    public static final int CLOSED   = 0;
    public static final int OPEN     = 1;
    
    public static byte[] CRLF = new byte[] { (byte)'\r', (byte)'\n' };

    public static byte[] EOF = new byte[0];

    public static final int LIMIT = 10;

    protected int channelState = PENDING;

    /**
     * Stores writes waiting to be sent over the wire.
     */
    protected BlockingQueue<ChannelBuffer> queue = new LinkedBlockingDeque( LIMIT );

    /**
     * Stores the result of this IO operation.  Boolean.TRUE if it was success
     * or Throwable it it was a failure.
     */
    protected BlockingQueue<Object> result = new LinkedBlockingDeque( 1 );

    /**
     * True when the open() method has been called.
     */
    private boolean opened = false;

    /**
     * The HTTP request URI representing this client.
     */
    protected URI uri;

    /**
     * The HTTP request we sent and are about to send.
     */
    protected HttpRequest request;

    /**
     * Request that we close()
     */
    private boolean closed = false;

    /**
     * The cause of a failure.
     */
    protected Throwable cause = null;

    /**
     * Clear to directly send a packet. 
     */
    protected boolean clear = false;

    /**
     * The channel we are using (when connected).
     */
    protected Channel channel = null;

    public RemoteChunkWriterClient( List<Host> hosts, String path ) throws IOException {

        try {
            
            URI uri = new URI( String.format( "http://%s%s", hosts.get(0), path ) );

            init( uri );
            
            String x_pipeline = "";
            
            for( int i = 1; i < hosts.size(); ++i ) {
                Host host = hosts.get( i );
                x_pipeline += host + " ";
            }

            x_pipeline = x_pipeline.trim();

            request.setHeader( FSHandler.X_PIPELINE_HEADER, x_pipeline );

        } catch ( URISyntaxException e ) {
            throw new IOException( e );
        }
        
    }

    public RemoteChunkWriterClient( URI uri ) throws IOException {
        init( uri );
    }
    
    public RemoteChunkWriterClient( HttpRequest request, URI uri ) throws IOException {
        this.request = request;
        this.uri = uri;
    }

    private void init( URI uri ) {

        String host = uri.getHost();

        // Prepare the HTTP request.
        this.request = new DefaultHttpRequest( HttpVersion.HTTP_1_1, HttpMethod.PUT, uri.toASCIIString() );

        request.setHeader( HttpHeaders.Names.USER_AGENT, RemoteChunkWriterClient.class.getName() );
        request.setHeader( HttpHeaders.Names.HOST, host );
        request.setHeader( HttpHeaders.Names.TRANSFER_ENCODING, "chunked" );

        this.uri = uri;

    }
    
    private void open() throws IOException {

        String host = uri.getHost();
        int port = uri.getPort();
        
        // Configure the client.
        ClientBootstrap bootstrap = new ClientBootstrap( socketChannelFactory );

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new RemoteChunkWriterClientPipelineFactory( this ) );

        // Start the connection attempt... where is the connect timeout set?
        ChannelFuture connectFuture = bootstrap.connect( new InetSocketAddress( host, port ) );

        // we're connected and can now perform IO by calling write.  We need to
        // pay attention to close so that we can not perform writes any longer.

        connectFuture.addListener( new ConnectFutureListener( this ) );

        opened = true;
        
    }

    public void setHeader( String name, String value ) {
        request.setHeader( name , value );
    }
    
    private void requireOpen() throws IOException {

        if ( ! opened ) open();
        
    }
    
    public void write( byte[] data ) throws IOException {

        write( ChannelBuffers.wrappedBuffer( data ) );
        
    }
    
    public void write( ChannelBuffer data ) throws IOException {

        requireOpen();
        
        data = newChannelBuffer( data );
        
        if ( closed || isChannelStateClosed() ) {

            if ( cause != null ) 
                throw new IOException( cause );
            
            throw new IOException( "closed" );

        }
        
        try {
            
            if ( clear ) {
                
                clear = false;
                channel.write( data ).addListener( new WriteFutureListener( this ) );
                
            } else {
                queue.put( data );
            }

        } catch ( Exception e ) {

            throw new IOException( e );

        }

    }
    
    public void close() throws IOException {

        // if we aren't opened there is no reason to do any work.  This could
        // happen if we opened this code and never did a write() to it which
        // would mean we don't have an HTTP connection to the server
        if ( ! opened ) return;

        // don't allow a double close.  This would never return.
        if ( closed ) return;
        
        //required for chunked encoding.
        write( EOF );

        // prevent any more write requests
        closed = true;

        try {

            // FIXME: this should have a timeout so that we don't block for
            // infinity.  We need a unit test for this.
            Object _result = result.take();

            if ( _result instanceof IOException )
                throw (IOException) result;

            if ( _result instanceof Throwable )
                throw new IOException( (Throwable)result );

            if ( _result.equals( Boolean.TRUE ) )
                return;

            throw new IOException( "unknown result: " + _result );

        } catch ( InterruptedException e ) {
            throw new IOException( e );
        }
            
    }

    public void setCause( Throwable throwable ) throws Exception {
        this.cause = throwable;
        this.result.put( throwable );
    }

    public void success() throws Exception {
        this.result.put( Boolean.TRUE );
    }

    public boolean isChannelStateClosed() {
        return channelState == CLOSED;
    }

    private ChannelBuffer newChannelBuffer( byte[] data ) {
        return newChannelBuffer( ChannelBuffers.wrappedBuffer( data ) );
    }
    
    private ChannelBuffer newChannelBuffer( ChannelBuffer data ) {

        // FIXME: we should use a Netty composite buffer to avoid the copy.

        String prefix = String.format( "%2x", data.writerIndex() );

        return ChannelBuffers.wrappedBuffer( ChannelBuffers.wrappedBuffer( prefix.getBytes() ),
                                             ChannelBuffers.wrappedBuffer( CRLF ),
                                             data,
                                             ChannelBuffers.wrappedBuffer( CRLF ) );
        
    }

    public String toString() {
        return uri.toString();
    }
    
}

class ConnectFutureListener implements ChannelFutureListener {

    private RemoteChunkWriterClient client;

    public ConnectFutureListener( RemoteChunkWriterClient client ) {
        this.client = client;
    }

    public void operationComplete( ChannelFuture future ) 
        throws Exception {

        client.channel = future.getChannel();

        client.channelState = RemoteChunkWriterClient.OPEN;

        // we need to find out when we are closed now.
        client.channel.getCloseFuture().addListener( new CloseFutureListener( client ) );

        client.channel.write( client.request ).addListener( new WriteFutureListener( client ) );

    }
        
}

class CloseFutureListener implements ChannelFutureListener {

    private RemoteChunkWriterClient client;
    
    public CloseFutureListener( RemoteChunkWriterClient client ) {
        this.client = client;
    }

    public void operationComplete( ChannelFuture future ) 
        throws Exception {

        client.channelState = RemoteChunkWriterClient.CLOSED;

        Throwable cause = future.getCause();
        
        if ( cause != null ) {
            client.setCause( cause );
        }

    }

}

class WriteFutureListener implements ChannelFutureListener {

    private RemoteChunkWriterClient client;
    
    public WriteFutureListener( RemoteChunkWriterClient client ) {
        this.client = client;
    }

    public void operationComplete( ChannelFuture future ) 
        throws Exception {

        Channel channel = future.getChannel();
        
        if ( client.queue.peek() != null ) {

             ChannelBuffer data = client.queue.take();

            // NOTE that even if the last response was written here we MUST wait
            // until we get the HTTP response.

            channel.write( data ).addListener( this );

            return;
            
        }

        // the queue was drained so the next packet should be sent directly

        client.clear = true;

    }

}