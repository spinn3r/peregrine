package peregrine.pfs;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;

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
public class WriterClient {

    public static void main(String[] args) throws Exception {

        URI uri = new URI( "http://localhost:11112/foo" );

        int port = uri.getPort();

        String host = uri.getHost();

        // Configure the client.
        ClientBootstrap bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new WriterClientPipelineFactory());

        // Start the connection attempt.
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));

        // Wait until the connection attempt succeeds or fails.
        Channel channel = future.awaitUninterruptibly().getChannel();

        System.out.printf( "channel: %s\n", channel.getClass().getName() );
        
        if ( ! future.isSuccess() ) {

            //FIXME: we need to throw an exception here I think... actually what
            //I NEED to do is have a listener handle this.
            
            future.getCause().printStackTrace();
            bootstrap.releaseExternalResources();
            return;
        }

        // Prepare the HTTP request.
        HttpRequest request = new DefaultHttpRequest( HttpVersion.HTTP_1_1, HttpMethod.PUT, uri.toASCIIString() );

        request.setHeader(HttpHeaders.Names.USER_AGENT, WriterClient.class.getName() );
        request.setHeader(HttpHeaders.Names.HOST, host);
        request.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, "chunked" );

        // Send the HTTP request.

        /*
        System.out.printf( "----\n" );

        byte[] data = new byte[ request.getContent().readableBytes() ];
        request.getContent().getBytes( 0, data );
        
        System.out.printf( "%s", Hex.pretty( data ) );
        System.out.printf( "----\n" );
        */
        
        channel.write( request ).addListener( new ChannelFutureListener() {

                int idx = 0;
                
                public void operationComplete( ChannelFuture future ) {

                    System.out.printf( "WRITE completed: %,d %s\n" , idx, future );

                    if ( ! future.getChannel().isWritable() ) {
                        System.out.printf( "NO LONGER WRITABLE.\n" );
                        return;
                        
                    }

                    if ( idx <= 500 ) {

                        ChannelBuffer cbuff = ChannelBuffers.dynamicBuffer();

                        String msg = "hello world\n";
                        
                        cbuff.writeBytes( String.format( "%2x\r\n", msg.length() ).getBytes() );
                        cbuff.writeBytes( msg.getBytes() );
                        cbuff.writeBytes( "\r\n".getBytes() );
                        
                        future.getChannel().write( cbuff ).addListener( this );

                    } else {

                        System.out.printf( "Writing LAST packet\n" );
                        
                        ChannelBuffer cbuff = ChannelBuffers.dynamicBuffer();

                        cbuff.writeBytes( "0\r\n\r\n".getBytes() );

                        future.getChannel().write( cbuff );
                        
                    }

                    ++idx;

                }
                
            } );

        channel.getCloseFuture().awaitUninterruptibly();
        
        // Shut down executor threads to exit.
        bootstrap.releaseExternalResources();
    }

}
