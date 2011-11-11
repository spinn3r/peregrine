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
import peregrine.config.*;
import peregrine.rpc.*;
import peregrine.shuffle.receiver.*;

import com.spinn3r.log5j.Logger;
import peregrine.util.netty.*;

public class FSDaemon {

    private static final Logger log = Logger.getLogger();
    
    public static final ExecutorService executors =
            Executors.newCachedThreadPool( new DefaultThreadFactory( HeartbeatSender.class) );
    
    private ServerBootstrap bootstrap = null;

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

        log.info( "Starting up... port: %s, root: %s" , port, root );
        
        // Bind and start to accept incoming connections.
        channel = bootstrap.bind( new InetSocketAddress( port ) );
        
        /*
        if ( ! config.getHost().equals( config.getController() ) )
        	executors.submit( new HeartbeatSender() );
        */
        
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
    
    public void shutdown() {

        String msg = String.format( "Shutting down PFSd on: %s", config.getHost() );
        
        log.info( "%s" , msg );

        channel.close().awaitUninterruptibly();

        log.debug( "Channel closed." );
        
        executors.shutdown();

        log.debug( "Executors shut down." );
        
        bootstrap.releaseExternalResources();

        log.info( "%s COMPLETE" , msg );
        
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
