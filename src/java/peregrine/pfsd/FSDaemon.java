package peregrine.pfsd;

import java.io.*;
import java.util.*;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.spinn3r.log5j.Logger;

public class FSDaemon {

    private static final Logger log = Logger.getLogger();

    public static int PORT = 11112;

    private ServerBootstrap bootstrap = null;

    private int port = -1;
    
    public FSDaemon() {
        this( PORT );
    }
    
    public FSDaemon( int port ) {

        this.port = port;
        
        // Configure the server.
        bootstrap = new ServerBootstrap( new NioServerSocketChannelFactory( Executors.newCachedThreadPool(),
                                                                            Executors.newCachedThreadPool() ) );

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new FSPipelineFactory() );

        log.info( "Starting on port: %s" , port );
        
        // Bind and start to accept incoming connections.
        bootstrap.bind( new InetSocketAddress( port ) );

    }

    public void shutdown() {

        log.info( "Shutting down on port: %s", port );
        bootstrap.releaseExternalResources();
        
    }

    static {

        // perform this once per VM.
        
        // first configure netty to use  
        InternalLoggerFactory.setDefaultFactory( new Log4JLoggerFactory() );

    }

}
    