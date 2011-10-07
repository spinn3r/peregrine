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

/**
 * A simple HTTP client that prints out the content of the HTTP response to
 * {@link System#out} to test {@link HttpServer}.
 *
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author Andy Taylor (andy.taylor@jboss.org)
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 *
 * @version $Rev: 2226 $, $Date: 2010-03-31 11:26:51 +0900 (Wed, 31 Mar 2010) $
 */
public class RemoteChunkWriterClient {

    public static byte[] CRLF = new byte[] { (byte)'\r', (byte)'\n' };

    public static final int LIMIT = 100;

    private static NioClientSocketChannelFactory socketChannelFactory =
        new NioClientSocketChannelFactory( Executors.newCachedThreadPool(), 
                                           Executors.newCachedThreadPool() );

    private Channel channel = null;

    /**
     * True when it is clear to send another packet.
     */
    private boolean clear = true;

    private WriteListener listener = new WriteListener();
    
    public RemoteChunkWriterClient( URI uri ) throws IOException {

        int port = uri.getPort();

        String host = uri.getHost();

        // Configure the client.
        ClientBootstrap bootstrap = new ClientBootstrap( socketChannelFactory );

        // Prepare the HTTP request.
        HttpRequest request = new DefaultHttpRequest( HttpVersion.HTTP_1_1, HttpMethod.PUT, uri.toASCIIString() );

        request.setHeader( HttpHeaders.Names.USER_AGENT, RemoteChunkWriterClient.class.getName() );
        request.setHeader( HttpHeaders.Names.HOST, host );
        request.setHeader( HttpHeaders.Names.TRANSFER_ENCODING, "chunked" );

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new RemoteChunkWriterClientPipelineFactory( request ) );

        // Start the connection attempt... where is the connect timeout set?
        ChannelFuture future = bootstrap.connect( new InetSocketAddress(host, port) );

        // Wait until the connection attempt succeeds or fails.
        this.channel = future.awaitUninterruptibly().getChannel();
                
        if ( ! future.isSuccess() ) {

            //FIXME: we need to throw an exception here I think... actually what
            //I NEED to do is have a listener handle this.

            throw new IOException( future.getCause() );

        } else {
            // FIXME: get the HTTP response so we can figure out what the heck
            // is going on here.
        }

    }

    public void write( byte[] data ) throws IOException {

        try {
            
            if ( listener.clear ) {
                
                listener.clear = false;
                channel.write( RemoteChunkWriterClient.newChannelBuffer( data ) ).addListener( listener );
                
            } else {
                listener.queue.put( data );
            }

        } catch ( Exception e ) {

            throw new IOException( e );

        }

    }

    //public static ChannelBuffer 
    
    public void close() throws IOException {

        // finish waiting for the queue to drain.. then close the connection.
        channel.close().awaitUninterruptibly();
        
    }

    public static ChannelBuffer newChannelBuffer( byte[] data ) {

        ChannelBuffer cbuff = ChannelBuffers.dynamicBuffer();
        
        cbuff.writeBytes( String.format( "%2x", data.length ).getBytes() );
        cbuff.writeBytes( CRLF );
        cbuff.writeBytes( data );
        cbuff.writeBytes( CRLF );

        return cbuff;
        
    }
    
    public static void main(String[] args) throws Exception {
        
        URI uri = new URI( "http://localhost:11112/foo" );

        RemoteChunkWriterClient client = new RemoteChunkWriterClient( uri );

        int block = 16384;
        
        long max = 150 * 1024;

        long nr_bytes = (long)max * (long)block;
        
        System.out.printf( "Writing %,d bytes.\n" , nr_bytes );
        
        long before = System.currentTimeMillis();
        
        for( int i = 0; i < max; ++i ) {
            client.write( new byte[block] );
        }

        client.close();

        long after = System.currentTimeMillis();

        long duration = after - before;
        
        System.out.printf( "duration: %,d ms\n", duration );
        
        System.out.printf( "closed.  sweet.\n" );
        
        socketChannelFactory.releaseExternalResources();

    }
        
}

class WriteListener implements ChannelFutureListener {

    public static final int LIMIT = 10;
    
    protected boolean clear = true;

    protected BlockingQueue<byte[]> queue = new LinkedBlockingDeque( LIMIT );
    
    public void operationComplete( ChannelFuture future ) 
        throws Exception {

        if ( queue.peek() != null ) {
            byte[] data = queue.take();

            ChannelBuffer cbuff = RemoteChunkWriterClient.newChannelBuffer( data );
            
            future.getChannel().write( cbuff ).addListener( this );
            
            return;
        }

        // the queue was drained so the next packet should be sent directly

        clear = true;
        
    }

}

        // Send the HTTP request.

        /*
        System.out.printf( "----\n" );

        byte[] data = new byte[ request.getContent().readableBytes() ];
        request.getContent().getBytes( 0, data );
        
        System.out.printf( "%s", Hex.pretty( data ) );
        System.out.printf( "----\n" );
        */

        /*
        
        final ChannelBuffer cbuff = ChannelBuffers.dynamicBuffer();

        StringBuffer sbuff = new StringBuffer();
        for ( int i = 0; i < 16384; ++i ) {
            sbuff.append( "x" );
        }
        
        String msg = sbuff.toString();
        
        cbuff.writeBytes( String.format( "%2x\r\n", msg.length() ).getBytes() );
        cbuff.writeBytes( msg.getBytes() );
        cbuff.writeBytes( "\r\n".getBytes() );

        final int max = 100;

        ChannelFutureListener listener =  new ChannelFutureListener() {

                int idx = 0;
                public void operationComplete( ChannelFuture future ) {

                    Channel channel = future.getChannel();

                    if ( ! channel.isOpen() ) {
 
                        if ( future.getCause() != null ) {
                            System.out.printf( "CAUGHT EXCEPTION\n" );
                            future.getCause().printStackTrace();
                        }
                        
                        return;

                    }
                    
                    if ( idx <= max ) {

                        System.out.printf( "." );
                        
                        channel.write( cbuff ).addListener( this );

                    } else {

                        ChannelBuffer cbuff = ChannelBuffers.dynamicBuffer();

                        cbuff.writeBytes( "0\r\n\r\n".getBytes() );

                        channel.write( cbuff );
                        
                    }

                    ++idx;

                }
                
            };

        */
//    }
