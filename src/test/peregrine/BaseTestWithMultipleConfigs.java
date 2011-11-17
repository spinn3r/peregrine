package peregrine;

import java.io.*;
import java.util.*;
import peregrine.config.*;
import peregrine.pfsd.*;

import com.spinn3r.log5j.Logger;

public abstract class BaseTestWithMultipleConfigs extends peregrine.BaseTest {

    private static final Logger log = Logger.getLogger();

    //public static int[] CONCURRENCY  = new int[] { 1, 2, 4, 8 };
    //    public static int[] CONCURRENCY  = new int[] { 2, 4, 8 };
    public static int[] CONCURRENCY  = new int[] { 1 };

    //FIXME: does not yet work with 3 replicas 
    //public static int[] REPLICAS     = new int[] { 1, 2, 3 };
    
    public static int[] REPLICAS     = new int[] { 1, 2 };
    public static int[] HOSTS        = new int[] { 1, 2, 4, 8 };

    protected Host controller;

    protected Config config;

    protected List<FSDaemon> daemons = new ArrayList();

    protected List<Config> configs = new ArrayList();

    protected Map<Host,Config> configsByHost;
    
    protected int concurrency = 0;
    protected int replicas = 0;
    protected int hosts = 0;

    protected int pass = -1;
    
    public void setUp() {

        super.setUp();

        daemons = new ArrayList();
        configs = new ArrayList();
        configsByHost = new HashMap();
        
        if ( concurrency == 0 )
            return;
        
        log.info( "Working with concurrency=%s, replicas=%s, hosts=%s" , concurrency, replicas, hosts );

        controller = new Host( "localhost", 11111 );
        config = newConfig( "localhost", 11111 );

        for( int i = 0; i < hosts; ++i ) {

            Config config = newConfig( "localhost", Config.DEFAULT_PORT + i );
            configs.add( config );

            configsByHost.put( config.getHost() , config );
            
            daemons.add( new FSDaemon( config ) );

        }

        log.info( "Working with configs %s and daemons %s" , configs, daemons );

    }

    public void tearDown() {

        for( FSDaemon daemon : daemons ) {
            daemon.shutdown();
        }

        daemons = new ArrayList();
        configs = new ArrayList();

        super.tearDown();

        showRunningThreads();
        
    }

    public void test() throws Exception {
        
        for( int concurrency : CONCURRENCY ) {

            for( int replicas : REPLICAS ) {

                for( int hosts : HOSTS ) {

                    this.concurrency = concurrency;
                    this.replicas = replicas;
                    this.hosts = hosts;

                    boolean ran = true;
                    
                    try {

                        ++pass;

                        setUp();

                        log.info( "Running with config: %s" , config );
                        
                        doTest();

                    } catch ( PartitionLayoutException e ) {

                        --pass;
                        ran = false;
                        
                        // this is just an invalid config so skip it.
                        log.warn( "Invalid config: %s" , e.getMessage() );
                        continue;

                    } catch ( Throwable t ) {
                        throw new Exception( String.format( "Test failed on pass %,d with config: ", pass, config ), t );
                    } finally {

                        tearDown();

                        if ( ran ) {
                            // create a copy of the logs for this task for debug 
                            copy( new File( "logs/peregrine.log" ), new File( String.format( "logs/test-%s-pass-%02d.log", getClass().getName(), pass ) ) );
                            
                            new FileOutputStream( "logs/peregrine.log" ).getChannel().truncate( 0 );
                        }

                    }

                }
                
            }
            
        }
        
    }

    protected Config newConfig( String host, int port ) {

        Config config = new Config( host, port );

        config.setController( controller );
        config.setConcurrency( concurrency );
        config.setReplicas( replicas );

        for( int i = 0; i < hosts; ++i ) {
            config.getHosts().add( new Host( "localhost", Config.DEFAULT_PORT + i ) );
        }
        
        config.init();
        
        return config;
        
    }

    /**
     * The actual test we want to run....
     */
    public abstract void doTest() throws Exception;

    public void showRunningThreads() {

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

        for( Thread thread : threadlist ) {
            System.out.printf( "  %s\n", thread.getName() );
        }
        
    }
    
}