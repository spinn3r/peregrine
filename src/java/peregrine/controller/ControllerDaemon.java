package peregrine.controller;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;

import peregrine.config.*;
import peregrine.pfsd.rpc.*;
import peregrine.rpc.*;
import peregrine.shuffle.receiver.*;
import peregrine.task.*;
import peregrine.util.*;
import peregrine.pfsd.*;

import com.spinn3r.log5j.Logger;
import peregrine.util.netty.*;
import peregrine.*;

public class ControllerDaemon {

    private static final Logger log = Logger.getLogger();
    
    private ServerBootstrap bootstrap = null;

    private ExecutorServiceManager executorServiceManager = new ExecutorServiceManager();
    
    private int port;

    private Channel channel;

    private Config config;

    private Scheduler scheduler = null;

    private Controller controller = null;
    
    public ControllerDaemon( Controller controller, Config config ) {

        this.controller = controller;
        this.config = config;
        this.port = config.getHost().getPort();
        
        String root = config.getRoot();
        
        File file = new File( root );

        // try to make the root directory.
        if ( ! file.exists() ) {
            file.mkdirs();
        }
        
        ThreadFactory tf = new DefaultThreadFactory( FSDaemon.class );
        
        int concurrency = config.getConcurrency();
        
        NioServerSocketChannelFactory factory = 
        		new NioServerSocketChannelFactory( FSDaemon.newDefaultThreadPool( concurrency, tf ),
                                                   FSDaemon.newDefaultThreadPool( concurrency, tf ),
                                                   concurrency ) ;
        		
        bootstrap = BootstrapFactory.newServerBootstrap( factory );

        // set up the event pipeline factory.
        bootstrap.setPipelineFactory( new ControllerPipelineFactory( controller, config ) );

        log.info( "Starting up on %s with root: %s" , config.getHost(), root );
        
        // Bind and start to accept incoming connections.
        channel = bootstrap.bind( new InetSocketAddress( port ) );

        log.info( "Now listening on %s with root: %s" , config.getHost(), root );
        
    }
    
    public Scheduler getScheduler() { 
        return this.scheduler;
    }

    public void setScheduler( Scheduler scheduler ) { 
        this.scheduler = scheduler;
    }

    public Config getConfig() {
    	return config;
    }

    public ExecutorService getExecutorService( Class clazz ) {
        return executorServiceManager.getExecutorService( clazz );
    }

    public void shutdown() {

        String msg = String.format( "Shutting down PFSd on: %s", config.getHost() );
        
        log.info( "%s" , msg );

        channel.close().awaitUninterruptibly();

        log.debug( "Channel closed." );

        executorServiceManager.shutdownAndAwaitTermination();
        
        log.info( "Releasing netty external resources" );
        
        bootstrap.releaseExternalResources();

        log.info( "%s COMPLETE" , msg );
        
    }

    @Override
    public String toString() {
        return String.format( "%s: %s", getClass().getSimpleName(), config.getHost() );
    }
        
    static {

        // first configure netty to use  
        InternalLoggerFactory.setDefaultFactory( new Log4JLoggerFactory() );

    }

}
