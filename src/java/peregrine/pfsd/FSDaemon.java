package peregrine.pfsd;

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

import com.spinn3r.log5j.Logger;
import peregrine.util.netty.*;

public class FSDaemon {

    private static final Logger log = Logger.getLogger();
    
    private ServerBootstrap bootstrap = null;

    private Map<Class,ExecutorService> executorServices = new ConcurrentHashMap();
    
    private int port;

    private Channel channel;
    
    /**
     * Each daemon can only have one shuffle instance.
     */
    public ShuffleReceiverFactory shuffleReceiverFactory;

    private Config config;

    private Scheduler scheduler = null;

    private HeartbeatTimer heartbeatTimer = null;

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
        
        // http://docs.jboss.org/netty/3.2/api/org/jboss/netty/channel/ChannelConfig.html
        
        NioServerSocketChannelFactory factory = 
        		new NioServerSocketChannelFactory( newDefaultThreadPool( tf ),
                                                   newDefaultThreadPool( tf ),
                                                   config.getConcurrency() ) ;
        		
        bootstrap = BootstrapFactory.newServerBootstrap( factory );

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new FSPipelineFactory( config, this ) );

        log.info( "Starting up on %s with root: %s" , config.getHost(), root );
        
        // Bind and start to accept incoming connections.
        channel = bootstrap.bind( new InetSocketAddress( port ) );

        log.info( "Now listening on %s with root: %s" , config.getHost(), root );
        
        if ( config.getController() != null &&
             ! config.getHost().equals( config.getController() ) ) {

            heartbeatTimer = new HeartbeatTimer( config );

        }
        
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

    /**
     * Create a default thread pool for use in the system with the correct
     * concurrency.
     */
    public ExecutorService newDefaultThreadPool( Class clazz ) {
        return newDefaultThreadPool( new DefaultThreadFactory( clazz ) );
    }

    /**
     * Create a default thread pool for use in the system with the correct
     * concurrency.
     */
    public ExecutorService newDefaultThreadPool( ThreadFactory threadFactory ) {

        return newDefaultThreadPool( config.getConcurrency(), threadFactory );

    }

    /**
     * Create a default thread pool for use in the system with the correct
     * concurrency.
     */
    public static ExecutorService newDefaultThreadPool( int concurrency, ThreadFactory threadFactory ) {

        ExecutorService service
            = Executors.newFixedThreadPool( concurrency , threadFactory );

        ThreadPoolExecutor tpe = (ThreadPoolExecutor)service;

        tpe.setKeepAliveTime( Long.MAX_VALUE, TimeUnit.MILLISECONDS );
        tpe.setCorePoolSize( concurrency );
        tpe.prestartAllCoreThreads();
        
        return service;

    }

    public ExecutorService getExecutorService( Class clazz ) {

        ExecutorService result;

        result = executorServices.get( clazz );

        // double check idiom
        if ( result == null ) {

            synchronized( executorServices ) {

                result = executorServices.get( clazz );
                
                if ( result == null ) {

                    result = Executors.newCachedThreadPool( new DefaultThreadFactory( clazz ) );
                    executorServices.put( clazz, result );
                    
                }
                
            }
            
        }

        return result;
        
    }

    public void shutdown() {

        String msg = String.format( "Shutting down PFSd on: %s", config.getHost() );
        
        log.info( "%s" , msg );

        channel.close().awaitUninterruptibly();

        log.debug( "Channel closed." );

        if ( heartbeatTimer != null )
            heartbeatTimer.cancel();
        
        for( Class clazz : executorServices.keySet() ) {

            log.info( "Shutting down executor service: %s", clazz.getName() );

            try {
                
                ExecutorService current = executorServices.get( clazz );
                current.shutdown();
                current.awaitTermination( Long.MAX_VALUE, TimeUnit.MILLISECONDS );
                
            } catch ( InterruptedException e ) {
                throw new RuntimeException( e );
            }

        }

        log.info( "Releasing netty external resources" );
        
        bootstrap.releaseExternalResources();

        log.info( "%s COMPLETE" , msg );
        
    }

    @Override
    public String toString() {
        return String.format( "FSDaemon: %s", config.getHost() );
    }
        
    static {

        // perform this once per VM.
        
        // first configure netty to use  
        InternalLoggerFactory.setDefaultFactory( new Log4JLoggerFactory() );

    }

}
