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
package peregrine;

import java.io.*;
import java.util.*;
import peregrine.config.*;
import peregrine.worker.*;

import com.spinn3r.log5j.Logger;

public abstract class BaseTestWithMultipleConfigs extends peregrine.BaseTest {

    private static final Logger log = Logger.getLogger();

    protected Host controller;

    protected Config config;

    protected List<FSDaemon> daemons = new ArrayList();

    protected List<Config> configs = new ArrayList();

    protected Map<Host,Config> configsByHost;
    
    protected int concurrency = 0;
    protected int replicas = 0;
    protected int hosts = 0;
    
    public void setUp() {

        super.setUp();

        String conf = System.getProperty( "peregrine.test.config" );

        if ( conf == null ) {
            log.warn( "NOT RUNNING: peregrine.test.config not defined" );
            return;
        }

        conf = conf.trim();
        
        String[] split = conf.split( ":" );

        concurrency = Integer.parseInt( split[0] );
        replicas = Integer.parseInt( split[1] );
        hosts = Integer.parseInt( split[2] );

        daemons = new ArrayList();
        configs = new ArrayList();
        configsByHost = new HashMap();
        
        if ( concurrency == 0 ) {
            log.error( "Concurrency was zero" );
            return;
        }
        
        log.info( "Working with concurrency=%s, replicas=%s, hosts=%s" , concurrency, replicas, hosts );

        controller = new Host( "localhost", 11111 );
        config = newConfig( "localhost", 11111 );

        for( int i = 0; i < hosts; ++i ) {

            Config config = newConfig( "localhost", Host.DEFAULT_PORT + i );
            configs.add( config );

            configsByHost.put( config.getHost() , config );
            
            daemons.add( new FSDaemon( config ) );

        }

        log.info( "Working with configs %s and daemons %s" , configs, daemons );

    }

    protected Config newConfig( String host, int port ) {

        Config config = new Config( host, port );

        config.setController( controller );
        config.setConcurrency( concurrency );
        config.setReplicas( replicas );

        for( int i = 0; i < hosts; ++i ) {
            config.getHosts().add( new Host( "localhost", Host.DEFAULT_PORT + i ) );
        }
        
        config.init();
        
        return config;
        
    }

    public void tearDown() {

        /*
        we don't need to do this anymore.
        shutdownAllDaemons();
        */
        
        daemons = new ArrayList();
        configs = new ArrayList();
        configsByHost = new HashMap();
        
        super.tearDown();

        //logRunningThreads();
        
    }

    public void shutdownAllDaemons() {

        log.info( "Shutting down %,d daemons", daemons.size() );
        
        for( FSDaemon daemon : daemons ) {

            log.info( "Shutting down: %s", daemons );
            daemon.shutdown();
        }

    }
    
    /**
     * Get the amount of work relative to the base test that we should be
     * working with.
     */
    public int getFactor() {

        String factor = System.getProperty( "peregrine.test.factor" );

        if ( factor == null )
            return 1;

        return Integer.parseInt( factor );
        
    }
    
    public void test() throws Exception {

        if ( config == null ) {
            log.warn( "Config was null.  Not running." );
            return;
        }
        
        try {

            log.info( "Running with config: %s" , config );
            
            doTest();

        } catch ( Throwable t ) {

            logRunningThreads();

            if ( t instanceof Exception )
                throw (Exception)t;

            if ( t instanceof RuntimeException )
                throw (RuntimeException)t;

            throw new RuntimeException( t );
            
        } finally {

            // create a copy of the logs for this task for debug 
            copy( new File( "logs/peregrine.log" ), new File( String.format( "logs/test-%s.log", getClass().getName() ) ) );
            
            new FileOutputStream( "logs/peregrine.log" ).getChannel().truncate( 0 );

        }

    }

    /**
     * The actual test we want to run....
     */
    public abstract void doTest() throws Exception;

    public void logRunningThreads() {

        ThreadGroup root = Thread.currentThread().getThreadGroup();

        while ( root.getParent() != null )
            root = root.getParent();

        Thread[] threads = new Thread[10];

        // WOW.  What a broken API.
        while( root.enumerate( threads, true ) == threads.length ) {
            threads = new Thread[ threads.length * 2 ];
        }

        List<Thread> threadlist = new ArrayList();

        for ( Thread thread : threads ) {

            if ( thread == null )
                break;

            threadlist.add( thread );
            
        }

        log.info( "%,d threads remain", threadlist.size() );

        StringBuilder buff = new StringBuilder();
        
        for( Thread thread : threadlist ) {
            
            buff.append( String.format( "%s\n", thread.getName() ) );

            for ( StackTraceElement frame : thread.getStackTrace() ) {
                buff.append( String.format( "\t%s\n", frame.toString() ) );
            }
            
        }

        log.info( "%s", buff.toString() );
        
    }
    
}
