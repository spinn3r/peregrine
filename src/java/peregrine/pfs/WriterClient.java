package peregrine.pfs;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;

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

        URI uri = new URI( "http://localhost:11112/test.dat" );

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

        if ( ! future.isSuccess() ) {

            //FIXME: we need to throw an exception here I think... actually what
            //I NEED to do is have a listener handle this.
            
            future.getCause().printStackTrace();
            bootstrap.releaseExternalResources();
            return;
        }

        // Prepare the HTTP request.
        HttpRequest request = new DefaultHttpRequest( HttpVersion.HTTP_1_1, HttpMethod.PUT, uri.toASCIIString() );
        request.setHeader(HttpHeaders.Names.HOST, host);
        request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);

        // Send the HTTP request.
        channel.write(request).addListener( new ChannelFutureListener() {

                public void operationComplete( ChannelFuture future ) {

                    System.out.printf( "WRITE completed.\n" );

                    System.out.printf( "closing\n" );

                    future.getChannel().close().addListener( new ChannelFutureListener() {
                            
                            public void operationComplete( ChannelFuture future ) {
                                
                                System.out.printf( "closed!\n" );    
                            }

                        } );

                }
                
            } );
        
        // Wait for the server to close the connection.
        channel.getCloseFuture().awaitUninterruptibly();

        // Shut down executor threads to exit.
        bootstrap.releaseExternalResources();
    }

}
