package peregrine.pfsd;

import java.io.*;
import java.util.*;

import java.net.InetSocketAddress;
import java.util.concurrent.*;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import peregrine.*;
import peregrine.util.*;
import peregrine.pfsd.shuffler.*;

import com.spinn3r.log5j.Logger;

public class FSDaemon {

    private static final Logger log = Logger.getLogger();

    public static int PORT = 11112;

    private ServerBootstrap bootstrap = null;

    private int port;

    /**
     * Each daemon can only have one shuffle instance.
     */
    public ShufflerFactory shufflerFactory;
    
    public FSDaemon( Config config ) {

        this.port = config.getHost().getPort();
        this.shufflerFactory = new ShufflerFactory( config ); 
        
        String root = config.getRoot();
        
        File file = new File( root );

        // try to make the root directory.
        if ( ! file.exists() ) {
            file.mkdirs();
        }
        
        ThreadFactory tf = new DefaultThreadFactory( FSDaemon.class );

        // Configure the server.
        bootstrap = new ServerBootstrap( new NioServerSocketChannelFactory( Executors.newCachedThreadPool( tf ),
                                                                            Executors.newCachedThreadPool( tf ) ) );

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new FSPipelineFactory( config, this ) );

        log.info( "Starting on port %s.  Using root: %s" , port, root );
        
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
