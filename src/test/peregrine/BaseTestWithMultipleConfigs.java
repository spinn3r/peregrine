package peregrine;

import java.io.*;
import java.util.*;
import peregrine.config.*;
import peregrine.pfsd.*;

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
        configsByHost = new HashMap();
        
        super.tearDown();

    }

    public void test() throws Exception {

        String conf = System.getProperty( "peregrine.config" );

        if ( conf == null )
            throw new RuntimeException( "peregrine.config not define" );

        conf = conf.trim();
        
        String[] split = conf.split( ":" );

        concurrency = Integer.parseInt( split[0] );
        replicas = Integer.parseInt( split[1] );
        hosts = Integer.parseInt( split[2] );

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

        } catch ( Throwable t ) {
            throw new Exception( String.format( "Test failed on pass %,d with config: %s", pass, config ), t );
        } finally {

            tearDown();

            if ( ran ) {
                // create a copy of the logs for this task for debug 
                copy( new File( "logs/peregrine.log" ), new File( String.format( "logs/test-%s-pass-%02d.log", getClass().getName(), pass ) ) );
                
                new FileOutputStream( "logs/peregrine.log" ).getChannel().truncate( 0 );
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

}