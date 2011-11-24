package peregrine.util.netty;

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

public abstract class BaseDaemon {

    private static final Logger log = Logger.getLogger();
    
    private ServerBootstrap bootstrap = null;

    private ExecutorServiceManager executorServiceManager = new ExecutorServiceManager();
    
    private Channel channel;

    private Config config;

    public void init() {

        String root = config.getRoot();
        
        new File( root ).mkdirs();
        
        ThreadFactory tf = new DefaultThreadFactory( getClass() );
        
        int concurrency = config.getConcurrency();
        
        NioServerSocketChannelFactory factory = 
        		new NioServerSocketChannelFactory( FSDaemon.newDefaultThreadPool( concurrency, tf ),
                                                   FSDaemon.newDefaultThreadPool( concurrency, tf ),
                                                   concurrency ) ;
        		
        bootstrap = BootstrapFactory.newServerBootstrap( factory );

        // set up the event pipeline factory.
        bootstrap.setPipelineFactory( getChannelPipelineFactory() );

        log.info( "Starting up on %s with root: %s" , config.getHost(), root );
        
        // Bind and start to accept incoming connections.
        channel = bootstrap.bind( new InetSocketAddress( config.getHost().getPort() ) );

        log.info( "Now listening on %s with root: %s" , config.getHost(), root );
        
    }

    public abstract ChannelPipelineFactory getChannelPipelineFactory();

    public Config getConfig() {
    	return config;
    }

    public void setConfig( Config config ) {
        this.config = config;
    }
    
    public ExecutorService getExecutorService( Class clazz ) {
        return executorServiceManager.getExecutorService( clazz );
    }

    public void shutdown() {

        String msg = String.format( "Shutting down daemon on: %s", config.getHost() );
        
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
