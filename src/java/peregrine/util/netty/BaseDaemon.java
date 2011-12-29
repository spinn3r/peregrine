/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.util.netty;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.logging.*;
import peregrine.*;
import peregrine.config.*;
import peregrine.shuffle.receiver.*;
import peregrine.task.*;
import peregrine.util.*;
import peregrine.util.netty.*;

import com.spinn3r.log5j.Logger;

public abstract class BaseDaemon {

    private static final Logger log = Logger.getLogger();
    
    private ServerBootstrap bootstrap = null;

    private ExecutorServiceManager executorServiceManager = new ExecutorServiceManager();
    
    private Channel channel;

    private Config config;

    public void init() {

        String root = config.getRoot();

        if ( root == null )
            throw new RuntimeException( "Root directory in config not defined." );
        
        new File( root ).mkdirs();
        
        ThreadFactory tf = new DefaultThreadFactory( getClass() );
        
        int concurrency = config.getConcurrency();
        
        NioServerSocketChannelFactory factory = 
        		new NioServerSocketChannelFactory( newDefaultThreadPool( concurrency, tf ),
                                                   newDefaultThreadPool( concurrency, tf ),
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

        /*
        ExecutorService service
            = Executors.newFixedThreadPool( concurrency , threadFactory );
        */

        ExecutorService service
            = Executors.newCachedThreadPool( threadFactory );

        ThreadPoolExecutor tpe = (ThreadPoolExecutor)service;

        tpe.setKeepAliveTime( Long.MAX_VALUE, TimeUnit.MILLISECONDS );
        tpe.setCorePoolSize( concurrency );
        tpe.prestartAllCoreThreads();
        
        return service;

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
