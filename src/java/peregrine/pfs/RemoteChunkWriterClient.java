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

import peregrine.config.Host;
import peregrine.io.async.*;
import peregrine.pfsd.*;
import peregrine.util.*;
import peregrine.util.netty.*;

/**
 * HTTP client that supports chunked PUT to a remote PFS node.
 * 
 */
public class RemoteChunkWriterClient extends BaseOutputStream implements ChannelBufferWritable {

    protected static NioClientSocketChannelFactory socketChannelFactory =
        new NioClientSocketChannelFactory( Executors.newCachedThreadPool( new DefaultThreadFactory( RemoteChunkWriterClient.class ) ), 
                                           Executors.newCachedThreadPool( new DefaultThreadFactory( RemoteChunkWriterClient.class ) ) );

    public static final String X_TAG = "X-tag";
    
    public static final int PENDING  = -1;
    public static final int CLOSED   = 0;
    public static final int OPEN     = 1;

    public static final int CHUNK_LENGTH_OVERHEAD = 10;
    
    public static final byte[] CRLF = new byte[] { (byte)'\r', (byte)'\n' };

    public static final int CHUNK_OVERHEAD = CHUNK_LENGTH_OVERHEAD + (CRLF.length * 2);

    public static final byte[] EOF = new byte[0];

    /**
     * Request tag which enables us to track down requests if they should fail.
     */
    public static long tag = 0;
    
    public static int LIMIT = 10;

    protected int channelState = PENDING;

    /**
     * Stores writes waiting to be sent over the wire.
     */
    protected BlockingQueue<ChannelBuffer> queue = new LinkedBlockingDeque( LIMIT );

    /**
     * Stores the result of this IO operation.  Boolean.TRUE if it was success
     * or Throwable it it was a failure.
     */
    protected BlockingQueue<Object> result = new LinkedBlockingDeque();

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

    protected HttpMethod method = HttpMethod.PUT;

    protected boolean inited = false;
    
    public RemoteChunkWriterClient( List<Host> hosts, String path ) throws IOException {

        try {
            
            this.uri = new URI( String.format( "http://%s%s", hosts.get(0), path ) );

            init();

            String x_pipeline = "";
            
            for( int i = 1; i < hosts.size(); ++i ) {
                Host host = hosts.get( i );
                x_pipeline += host + " ";
            }

            x_pipeline = x_pipeline.trim();

            if ( x_pipeline.length() != 0 )
                request.setHeader( FSHandler.X_PIPELINE_HEADER, x_pipeline );

        } catch ( URISyntaxException e ) {
            throw new IOException( e );
        }
        
    }

    public RemoteChunkWriterClient( String uri ) throws IOException {

        try {
            this.uri = new URI( uri );
        } catch ( URISyntaxException e ) {
            throw new IOException( e );
        }

    }

    public RemoteChunkWriterClient( URI uri ) throws IOException {
        this.uri = uri;
    }
    
    public RemoteChunkWriterClient( HttpRequest request, URI uri ) throws IOException {
        this.request = request;
        this.uri = uri;
    }

    public void init() {

        String host = uri.getHost();

        // Prepare the HTTP request.
        this.request = new DefaultHttpRequest( HttpVersion.HTTP_1_1, method, uri.toASCIIString() );

        request.setHeader( HttpHeaders.Names.USER_AGENT, RemoteChunkWriterClient.class.getName() );
        request.setHeader( HttpHeaders.Names.HOST, host );
        request.setHeader( HttpHeaders.Names.TRANSFER_ENCODING, "chunked" );

        request.setHeader( X_TAG, "" + tag++ );
        
        inited = true;
        
    }

    private void requireInit() {
        if ( ! inited ) init();
    }

    private void open() throws IOException {

        requireInit();
        
        String host = uri.getHost();
        int port = uri.getPort();
        
        // Configure the client.
        ClientBootstrap bootstrap = BootstrapFactory.newClientBootstrap( socketChannelFactory );

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new RemoteChunkWriterClientPipelineFactory( this ) );

        // Start the connection attempt... where is the connect timeout set?
        ChannelFuture connectFuture = bootstrap.connect( new InetSocketAddress( host, port ) );

        // we're connected and can now perform IO by calling write.  We need to
        // pay attention to close so that we can not perform writes any longer.

        connectFuture.addListener( new ConnectFutureListener( this ) );

        opened = true;
        
    }

    private void requireOpen() throws IOException {

        requireInit();

        if ( ! opened ) open();
        
    }

    public void setMethod( HttpMethod method ) {
        this.method = method;
    }
    
    public void setHeader( String name, String value ) {
        requireInit();
        request.setHeader( name , value );
    }
    
    public void write( byte[] data ) throws IOException {
        write( ChannelBuffers.wrappedBuffer( data ) );
    }

    public void failed( Throwable throwable ) throws Exception {

        this.cause = throwable;

        updateResult( throwable );

        this.channelState = CLOSED;

    }

    public void success() throws Exception {

        updateResult( Boolean.TRUE );
    }

    private void updateResult( Object value ) throws Exception {

        if ( result.size() >= 1 )
            throw new Exception( "Too many result messages: " + result );

        result.put( value );
        
    }
    
    public boolean isChannelStateClosed() {
        return channelState == CLOSED;
    }

    private ChannelBuffer newChannelBuffer( ChannelBuffer data ) {

        // use Netty composite buffers to avoid copying excessive data.
        String prefix = String.format( "%2x", data.writerIndex() );

        return ChannelBuffers.wrappedBuffer( ChannelBuffers.wrappedBuffer( prefix.getBytes() ),
                                             ChannelBuffers.wrappedBuffer( CRLF ),
                                             data,
                                             ChannelBuffers.wrappedBuffer( CRLF ) );
        
    }

    public String toString() {
        return uri.toString();
    }

    public void close() throws IOException {

        // if we aren't opened there is no reason to do any work.  This could
        // happen if we opened this code and never did a write() to it which
        // would mean we don't have an HTTP connection to the server
        if ( ! opened ) return;

        // don't allow a double close.  This would never return.
        if ( closed ) return;
        
        //required for chunked encoding.  We must wrote an EOF at the end of the
        //stream ot note that there is no more data.
        write( EOF );

        waitForClose();
        
        // prevent any more write requests
        closed = true;

        try {

            // FIXME: this should have a timeout so that we don't block for
            // infinity.  We need a unit test for this... this would basically
            // emulate infinte write timeouts.
            
            Object took = result.take();

            if ( took instanceof IOException )
                throw (IOException) took;

            if ( took instanceof Throwable )
                throw new IOException( (Throwable)took );

            if ( took.equals( Boolean.TRUE ) )
                return;

            throw new IOException( "unknown result: " + took );

        } catch ( InterruptedException e ) {
            throw new IOException( e );
        }
            
    }

    private void waitForClose() throws IOException {

        // FIXME: I don't like this code and it probably means that this entire
        // class should be refactored to avoid having to do this.  The problem
        // is that in the pipeline code I can't await() and must do ALL the IO
        // in the event thread.  This is normally OK but this ends up with a
        // race condition on shutdown where we haven't YET flagged the channel
        // as clear and then add the EOF to the queue but it's in the QUEUE so
        // it is never sent and we sit here blocking forever.  This is a
        // workaround but it would be nice to have a more elegant way to handle
        // this.
        while( true ) { 
        
            try {

                if ( clear && queue.peek() != null ) {

                    try {
                    
                        ChannelBuffer data = queue.take();
                        
                        channel.write( data ).addListener( new WriteFutureListener( this ) );
                        
                    } catch ( InterruptedException e ) {
                        throw new IOException( e );
                    }

                }

                if ( isChannelStateClosed() )
                    break;
                
                // TODO: this should be a constant ...
                Thread.sleep( 10L );

            } catch ( Exception e ) {
                throw new IOException( e );
            }

        }

    }
    
    public void write( ChannelBuffer data ) throws IOException {

        requireOpen();
        
        data = newChannelBuffer( data );
        
        if ( closed || isChannelStateClosed() ) {

            if ( cause != null ) 
                throw new IOException( cause );
            
            throw new IOException( String.format( "Write after closed: closed=%s, isChannelStateClosed: %s ",
                                                  closed, isChannelStateClosed() ) );

        }
        
        try {
            
            if ( clear ) {
                
                clear = false;

                // NOTE it is required to put a packet on to the queue and pull
                // one back off because technically there could be a race in the
                // write listener where we read it, it was empty, one was put on
                // the queue, then we were marked as clear.  We ALSO need to do
                // by first taking the head out of the queue and THEN putting an
                // item in as the queue MAY be full and we would block if we
                // tried to put an item in first.

                if ( queue.peek() != null ) {

                    ChannelBuffer tmp = queue.take();
                    queue.put( data );
                    data = tmp;

                }

                channel.write( data ).addListener( new WriteFutureListener( this ) );

            } else {
                queue.put( data );
            }

        } catch ( Exception e ) {

            throw new IOException( e );

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

            if ( channel.isConnected() == false ) 
                return; // the close future will handle proper shutdown.

            if ( future.isSuccess() == false )
                return;
                
            if ( client.queue.peek() != null ) {

                 ChannelBuffer data = client.queue.take();

                // NOTE that even if the last response was written here we MUST wait
                // until we get the HTTP response.
                 
                 channel.write( data ).addListener( new WriteFutureListener( client ) );

                 return;
                
            }

            // the queue was drained so the next packet should be sent direc

            client.clear = true;

        }

    }

    class ConnectFutureListener implements ChannelFutureListener {

        private RemoteChunkWriterClient client;

        public ConnectFutureListener( RemoteChunkWriterClient client ) {
            this.client = client;
        }

        public void operationComplete( ChannelFuture future ) 
            throws Exception {
                    	
        	if ( ! future.isSuccess() ) {
        		client.failed( future.getCause() );           
        		return;
        	}
        	
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
        	        	
            Throwable cause = future.getCause();

            if ( cause != null ) {
                client.failed( cause );
            }

        }

    }

}
