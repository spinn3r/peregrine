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
        		new NioServerSocketChannelFactory( Executors.newCachedThreadPool( tf ),
                                                   Executors.newCachedThreadPool( tf ) ) ;
        		
        bootstrap = BootstrapFactory.newServerBootstrap( factory );

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new FSPipelineFactory( config, this ) );

        log.info( "Starting up on %s with root: %s" , config.getHost(), root );
        
        // Bind and start to accept incoming connections.
        channel = bootstrap.bind( new InetSocketAddress( port ) );

        log.info( "Now listening on %s with root: %s" , config.getHost(), root );
        
        if ( ! config.getHost().equals( config.getController() ) )
        	getExecutorService( HeartbeatSender.class ).submit( new HeartbeatSender() );
        
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

        for( Class clazz : executorServices.keySet() ) {

            log.info( "Shutting down executor service: %s", clazz.getName() );
            executorServices.get( clazz ).shutdownNow();
            
        }

        bootstrap.releaseExternalResources();

        log.info( "%s COMPLETE" , msg );
        
    }

    @Override
    public String toString() {
        return String.format( "FSDaemon: %s", config.getHost() );
    }
    
    class HeartbeatSender implements Callable<Void>{
    	
        public static final long ONLINE_SLEEP_INTERVAL  = 30000L;
        
        public static final long OFFLINE_SLEEP_INTERVAL = 1000L;    	
    	
		@Override
		public Void call() throws Exception {

	        while( true ) {
	        	
	        	if ( sendHeartbeatToController() ) {

	                Thread.sleep( ONLINE_SLEEP_INTERVAL );
	        		
	        	} else {
	        		
	                Thread.sleep( OFFLINE_SLEEP_INTERVAL );
	        		
	        	}
	                        
	        }
			
		}
    	
	    public boolean sendHeartbeatToController() {
	    	
	        Message message = new Message();
	        message.put( "action", "heartbeat" );
	        message.put( "host",    config.getHost().toString() );
	        
	        Host controller = config.getController();
	        
	        try {        	
	        	
				new Client().invoke( controller, "controller", message );
				
				return true;
				
			} catch (IOException e) {
				
				log.warn( String.format( "Unable to send heartbeat to %s: %s", 
					      controller, e.getMessage() ) );
				
				return false;
			}
	   
	    }        	
    	
    }
    
    static {

        // perform this once per VM.
        
        // first configure netty to use  
        InternalLoggerFactory.setDefaultFactory( new Log4JLoggerFactory() );

    }

}
