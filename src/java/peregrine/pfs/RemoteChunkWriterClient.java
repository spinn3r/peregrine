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

/**
 * HTTP client that supports chunked PUT to a remote PFS node.
 * 
 */
public class RemoteChunkWriterClient extends BaseOutputStream {

    public static byte[] CRLF = new byte[] { (byte)'\r', (byte)'\n' };

    public static byte[] EOF = new byte[0];
    
    public static final int LIMIT = 100;

    private static NioClientSocketChannelFactory socketChannelFactory =
        new NioClientSocketChannelFactory( Executors.newCachedThreadPool( new DefaultThreadFactory( RemoteChunkWriterClient.class ) ), 
                                           Executors.newCachedThreadPool( new DefaultThreadFactory( RemoteChunkWriterClient.class ) ) );

    private RemoteChunkWriterClientListener listener;

    private boolean open = false;

    protected URI uri;

    protected HttpRequest request;

    private boolean closed = false;
    
    public RemoteChunkWriterClient( URI uri ) throws IOException {

        String host = uri.getHost();

        // Prepare the HTTP request.
        this.request = new DefaultHttpRequest( HttpVersion.HTTP_1_1, HttpMethod.PUT, uri.toASCIIString() );

        request.setHeader( HttpHeaders.Names.USER_AGENT, RemoteChunkWriterClient.class.getName() );
        request.setHeader( HttpHeaders.Names.HOST, host );
        request.setHeader( HttpHeaders.Names.TRANSFER_ENCODING, "chunked" );

        this.uri = uri;

    }
    
    public RemoteChunkWriterClient( HttpRequest request, URI uri ) throws IOException {

        this.request = request;
        this.uri = uri;
        
    }

    private void open() throws IOException {

        String host = uri.getHost();
        int port = uri.getPort();

        listener = new RemoteChunkWriterClientListener( this );
        
        // Configure the client.
        ClientBootstrap bootstrap = new ClientBootstrap( socketChannelFactory );

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new RemoteChunkWriterClientPipelineFactory( listener ) );

        // Start the connection attempt... where is the connect timeout set?
        ChannelFuture connectFuture = bootstrap.connect( new InetSocketAddress( host, port ) );

        // we're connected and can now perform IO by calling write.  We need to
        // pay attention to close so that we can not perform writes any longer.

        connectFuture.addListener( listener );

        open = true;
        
    }

    public void setHeader( String name, String value ) {
        request.setHeader( name , value );
    }
    
    private void requireOpen() throws IOException {

        if ( ! open ) open();
        
    }
    
    public void write( byte[] data ) throws IOException {

        write( ChannelBuffers.wrappedBuffer( data ) );
        
    }
    
    public void write( ChannelBuffer data ) throws IOException {

        requireOpen();
        
        data = newChannelBuffer( data );
        
        if ( closed || listener.isClosed() ) {

            if ( listener.cause != null ) 
                throw new IOException( listener.cause );

            System.out.printf( "WE WREE CLOSED\n" );
            
            throw new IOException( "closed" );

        }
        
        try {
            
            if ( listener.clear ) {
                
                listener.clear = false;
                listener.channel.write( data ).addListener( listener );
                
            } else {
                listener.queue.put( data );
            }

        } catch ( Exception e ) {

            throw new IOException( e );

        }

    }
    
    public void close() throws IOException {

        //required for chunked encoding.
        write( EOF );

        // prevent any more write requests
        closed = true;

        try {
            
            Object result = listener.result.take();

            if ( result instanceof IOException )
                throw (IOException) result;

            if ( result instanceof Throwable )
                throw new IOException( (Throwable)result );

            if ( result.equals( Boolean.TRUE ) )
                return;

            throw new IOException( "unknown result: " + result );

        } catch ( InterruptedException e ) {
            throw new IOException( e );
        }
            
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
        
}
