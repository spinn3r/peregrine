package peregrine.pfsd;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.concurrent.*;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import peregrine.util.*;
import peregrine.task.*;
import peregrine.config.Config;
import peregrine.shuffle.receiver.*;

import com.spinn3r.log5j.Logger;

public class FSDaemon {

    public static boolean TCP_NODELAY = true;

    public static long CONNECT_TIMEOUT_MILLIS = 5000;
    
    private static final Logger log = Logger.getLogger();

    private ServerBootstrap bootstrap = null;

    private int port;

    private Channel channel;
    
    /**
     * Each daemon can only have one shuffle instance.
     */
    public ShuffleReceiverFactory shuffleReceiverFactory;

    public Config config;

    private Scheduler scheduler = null;

    public FSDaemon( Config config ) {

        this.config = config;
        this.port = config.getHost().getPort();
        this.shuffleReceiverFactory = new ShuffleReceiverFactory( config ); 
        
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

        // set options 
        bootstrap.setOption( "tcpNoDelay", TCP_NODELAY );
        bootstrap.setOption( "connectTimeoutMillis", CONNECT_TIMEOUT_MILLIS );
        
        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new FSPipelineFactory( config, this ) );

        log.info( "Starting up... port: %s, root: %s" , port, root );
        
        // Bind and start to accept incoming connections.
        channel = bootstrap.bind( new InetSocketAddress( port ) );

    }

    public Scheduler getScheduler() { 
        return this.scheduler;
    }

    public void setScheduler( Scheduler scheduler ) { 
        this.scheduler = scheduler;
    }

    public void shutdown() {

        log.info( "Shutting down on port: %s", port );

        channel.close().awaitUninterruptibly();
        
        bootstrap.releaseExternalResources();
        
    }

    static {

        // perform this once per VM.
        
        // first configure netty to use  
        InternalLoggerFactory.setDefaultFactory( new Log4JLoggerFactory() );

    }

}
