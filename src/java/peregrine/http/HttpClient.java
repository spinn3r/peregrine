/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.http;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.config.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.worker.*;

import com.spinn3r.log5j.Logger;

/**
 * HTTP client that supports chunked PUT to a remote PFS node.
 * 
 */
public class HttpClient implements ChannelBufferWritable {

    private static final Logger log = Logger.getLogger();

    protected static DefaultThreadFactory threadFactory =
        new DefaultThreadFactory( HttpClient.class );
    
    // FIXME: change to a fixed size thread pool based on concurrency... 
    protected static NioClientSocketChannelFactory socketChannelFactory =
        new NioClientSocketChannelFactory( Executors.newCachedThreadPool( threadFactory ), 
                                           Executors.newCachedThreadPool( threadFactory ) );

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
    
    /**
     * Maximum number of writes to keep in the queue.
     */
    public static int QUEUE_CAPACITY = 10;

    /**
     * The write timeout for requests.  Due to GC pause and so forth when using
     * the ParallelGC this should probably be larger as we HAVE seen write
     * timeouts when setting it to 60 seconds.  
     *
     * <p>TODO: this should be a configuration directive.
     */
    public static final int WRITE_TIMEOUT = 300000;
    
    protected int channelState = PENDING;

    /**
     * Stores writes waiting to be sent over the wire.
     */
    protected SimpleBlockingQueue<ChannelBuffer> queue = new SimpleBlockingQueue( QUEUE_CAPACITY, WRITE_TIMEOUT );

    /**
     * Stores the result of this IO operation.  Boolean.TRUE if it was success
     * or Throwable it it was a failure.
     */
    protected BlockingQueue<Object> result = new LinkedBlockingDeque();

    /**
     * The HTTP request URI representing this client.
     */
    protected URI uri;

    /**
     * The HTTP request we sent and are about to send.
     */
    protected HttpRequest request;

    /**
     * True when the open method has been called.
     */
    private boolean opened = false;

    /**
     * Request that we close the connection.
     */
    private boolean closed = false;

    /**
     * True while we are closing.
     */
    private boolean closing = false;
    
    /**
     * The cause of a failure.
     */
    protected Throwable cause = null;

    /**
     * Clear to directly send a packet. 
     */
    protected SimpleBlockingQueue<Boolean> clearToSend = new SimpleBlockingQueue();

    /**
     * True when we have initialized the client.
     */
    protected boolean initialized = false;
    
    /**
     * The channel we are using (when connected).
     */
    protected Channel channel = null;

    /**
     * The default method to use (normally HTTP PUT).
     */
    protected HttpMethod method = HttpMethod.PUT;

    protected HttpResponse response = null;

    protected ChannelBuffer content = null;

    protected Config config = null;
    
    public HttpClient( Config config, List<Host> hosts, String path ) throws IOException {

        try {

            this.config = config;
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

    public HttpClient( Config config, String uri ) throws IOException {

        try {
            this.config = config;
            this.uri = new URI( uri );
        } catch ( URISyntaxException e ) {
            throw new IOException( e );
        }

    }

    public HttpClient( Config config, URI uri ) throws IOException {
        this.config = config;
        this.uri = uri;
    }
    
    public HttpClient( Config config, HttpRequest request, URI uri ) throws IOException {
        this.config = config;
        this.request = request;
        this.uri = uri;
    }

    public void init() {

        String host = uri.getHost();

        // Prepare the HTTP request.
        this.request = new DefaultHttpRequest( HttpVersion.HTTP_1_1, method, uri.toASCIIString() );

        request.setHeader( HttpHeaders.Names.USER_AGENT, HttpClient.class.getName() );
        request.setHeader( HttpHeaders.Names.HOST, host );
        request.setHeader( HttpHeaders.Names.TRANSFER_ENCODING, "chunked" );

        request.setHeader( X_TAG, "" + tag++ );
        
        initialized = true;
        
    }

    private void requireInit() {
        if ( ! initialized ) init();
    }

    private void open() throws IOException {

        requireInit();
        
        String host = uri.getHost();
        int port = uri.getPort();
        
        // Configure the client.
        ClientBootstrap bootstrap = BootstrapFactory.newClientBootstrap( socketChannelFactory );

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new HttpClientPipelineFactory( config, this ) );

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

    /**
     * Return the HTTP response.
     */
    public HttpResponse getResponse() {
        return response;
    }
    
    public void write( byte[] data ) throws IOException {
        write( ChannelBuffers.wrappedBuffer( data ) );
    }

    public void failed( Throwable throwable ) throws Exception {

        this.cause = throwable;

        updateResult( throwable );

        this.channelState = CLOSED;

        onClose( false, throwable );
        
    }

    public void success() throws Exception {

        updateResult( Boolean.TRUE );

        onClose( true, null );

    }

    private void updateResult( Object value ) throws Exception {

        if( result.contains( value ) ) {
            // if two listeners are called (for example fail on a write and then
            // the close future with the same cause) then we should ignroe the
            // second one.
            return;
        }
        
        if ( result.size() >= 1 )
            log.warn( "Too many result messages: " + result );

        result.put( value );
        
    }
    
    public boolean isChannelStateClosed() {
        return channelState == CLOSED;
    }

    private ChannelBuffer newChannelBuffer( ChannelBuffer data ) {

        // use Netty composite buffers to avoid copying excessive data.
        String prefix = String.format( "%2x", data.writerIndex() );
        
        ChannelBuffer result =
            ChannelBuffers.wrappedBuffer( ChannelBuffers.wrappedBuffer( prefix.getBytes() ),
                                          ChannelBuffers.wrappedBuffer( CRLF ),
                                          data,
                                          ChannelBuffers.wrappedBuffer( CRLF ) );

        return result;
        
    }

    @Override
    public String toString() {
        return String.format( "%s: %s %s", getClass().getSimpleName(), method, uri.toString() );
    }

    /**
     * Get the result of this HTTP request (True on success) or throw an
     * IOException on failure.
     * @return True on success, null on pending.
     * @throws IOException on failure
     */
    public ChannelBuffer getResult() throws IOException {

        Object peek = result.peek();

        return handleResult( peek );

    }
    
    private ChannelBuffer handleResult( Object peek ) throws IOException {

        if ( peek instanceof IOException )
            throw (IOException) peek;

        if ( peek instanceof Throwable )
            throw new IOException( (Throwable)peek );

        if ( peek instanceof Boolean ) {

            boolean result = ((Boolean)peek).booleanValue();

            if ( result )
                return content;
            else
                throw new IOException( "false" );

        }

        if ( peek != null )
            throw new IllegalArgumentException( "unknown value: " + peek );
        
        return content;

    }

    /**
     * Send a close request to the remote client.  This is non-blocking.
     */
    @Override
    public void shutdown() throws IOException {

        // if we aren't opened there is no reason to do any work.  This could
        // happen if we opened this code and never did a write() to it which
        // would mean we don't have an HTTP connection to the server
        if ( ! opened ) return;

        // don't allow a double close.  This would never return.
        if ( closed || closing ) return;

        closing = true;

        //required for chunked encoding.  We must write an EOF at the end of the
        //stream ot note that there is no more data.
        write( EOF );

    }

    // TODO: add this later as it would be nice to flush pending output but
    // right now it's not strictly needed.

    @Override
    public void flush() throws IOException { }

    @Override
    public void sync() throws IOException { }
    
    @Override
    public void close() throws IOException {
        close(true);
    }
    
    public void close( boolean block ) throws IOException {
        
        // if we aren't opened there is no reason to do any work.  This could
        // happen if we opened this code and never did a write() to it which
        // would mean we don't have an HTTP connection to the server
        if ( ! opened ) return;

        // don't allow a double close.  This would never return.
        if ( closed ) return;

        if ( closing == false )
            shutdown();

        // even in block mode we must enter this once to send the last packet
        // directly if necessary.
        waitForClose( block );

        // prevent any more write requests
        closed = true;

        // we need to return here becuase we would then block for the result.
        if ( block == false )
            return;
        
        try {

            // FIXME: this should have a timeout so that we don't block for
            // infinity.  We need a unit test for this... this would basically
            // emulate infinte write timeouts... same with waitForClose... 

            Object took = result.take();

            handleResult( took );
            
        } catch ( InterruptedException e ) {
            throw new IOException( e );
        }
            
    }

    private void waitForClose( boolean block ) throws IOException {

        // FIXME: I don't like this code and it probably means that this entire
        // class should be refactored to avoid having to do this.  The problem
        // is that in the pipeline code I can't await() and must do ALL the IO
        // in the event thread.  This is normally OK but this ends up with a
        // race condition on shutdown where we haven't YET flagged the channel
        // as clear and then add the EOF to the queue but it's in the QUEUE so
        // it is never sent and we sit here blocking forever.  This is a
        // workaround but it would be nice to have a more elegant way to handle
        // this.
        //
        // A BETTER way to handle this would be to have explicit shutdown
        // required by my code so that OUR code must shutdown FIRST instead of
        // allowing daemon threads to just exit without a defined order.  This
        // way all the event handlers will execute and then complete IO and
        // our main threads will terminate and then I can shutdown netty.

        long started = System.currentTimeMillis();

        while( true ) { 
        
            if ( clearToSend.peek() != null && queue.peek() != null ) {

                clearToSend.take();
                
                ChannelBuffer data = queue.take();
                
                channel.write( data ).addListener( new WriteFutureListener( this ) );

            }

            if ( isChannelStateClosed() || block == false )
                break;
            
            // TODO: this should be a constant ...
            try {
                Thread.sleep( 10L );
            } catch ( InterruptedException e ) {
                throw new IOException( e );
            }

            if ( System.currentTimeMillis() - started >= WRITE_TIMEOUT ) {
                throw new IOException( String.format( "write timeout: %s (%s)", WRITE_TIMEOUT, result ) );
            }

        }

    }

    private ChannelBuffer takeFromQueue() throws InterruptedException {

        ChannelBuffer result = queue.take();
        
        onCapacityChange( queue.remainingCapacity() != 0 );

        return result;
        
    }
    
    public void write( ChannelBuffer data ) throws IOException {

        if ( ! closing && data.writerIndex() == 0 )
            return;
        
        requireOpen();
        
        data = newChannelBuffer( data );
        
        if ( closed || isChannelStateClosed() ) {

            if ( cause != null ) 
                throw new IOException( cause );
            
            throw new IOException( String.format( "Write after closed: closed=%s, isChannelStateClosed: %s ",
                                                  closed, isChannelStateClosed() ) );

        }
        
        try {
            
            if ( clearToSend.peek() != null ) {
                
                clearToSend.take();

                // NOTE it is required to put a packet on to the queue and pull
                // one back off because technically there could be a race in the
                // write listener where we read it, it was empty, one was put on
                // the queue, then we were marked as clear.  We ALSO need to do
                // by first taking the head out of the queue and THEN putting an
                // item in as the queue MAY be full and we would block if we
                // tried to put an item in first.

                if ( queue.peek() != null ) {

                    ChannelBuffer tmp = takeFromQueue();
                    queue.put( data );
                    data = tmp;

                }

                channel.write( data ).addListener( new WriteFutureListener( this ) );

            } else {
                queue.put( data );
            }

            onCapacityChange( queue.remainingCapacity() != 0 );
            
        } catch ( Exception e ) {

            throw new IOException( e );

        }

    }

    // TODO: move these into an event listener... 

    /**
     * Called when the capacity for the underlying queue changes so that we can
     * change the readable status of channels, etc.
     */
    public void onCapacityChange( boolean hasCapacity ) {

    }

    /**
     * Called when the client is closed.  
     */
    public void onClose( boolean success, Throwable cause ) {

    }
    
    class WriteFutureListener implements ChannelFutureListener {

        private HttpClient client;
        
        public WriteFutureListener( HttpClient client ) {
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

                 ChannelBuffer data = client.takeFromQueue();

                // NOTE that even if the last response was written here we MUST wait
                // until we get the HTTP response.
                 
                 channel.write( data ).addListener( new WriteFutureListener( client ) );

                 return;
                
            }

            // the queue was drained so the next packet should be sent direc

            client.clearToSend.put( Boolean.TRUE );

        }

    }

    class ConnectFutureListener implements ChannelFutureListener {

        private HttpClient client;

        public ConnectFutureListener( HttpClient client ) {
            this.client = client;
        }

        public void operationComplete( ChannelFuture future ) 
            throws Exception {
                    	
        	if ( ! future.isSuccess() ) {
        		client.failed( future.getCause() );           
        		return;
        	}
        	
            client.channel = future.getChannel();

            client.channelState = HttpClient.OPEN;

            // we need to find out when we are closed now.
            client.channel.getCloseFuture().addListener( new CloseFutureListener( client ) );

            client.channel.write( client.request ).addListener( new WriteFutureListener( client ) );

        }
            
    }

    class CloseFutureListener implements ChannelFutureListener {

        private HttpClient client;
        
        public CloseFutureListener( HttpClient client ) {
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
