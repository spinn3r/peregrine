package peregrine;

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

    protected int concurrency = 0;
    protected int replicas = 0;
    protected int hosts = 0;
    
    public void setUp() {

        super.setUp();

        daemons = new ArrayList();
        configs = new ArrayList();

        if ( concurrency == 0 )
            return;
        
        log.info( "Working with concurrency=%s, replicas=%s, hosts=%s" ,
                  concurrency, replicas, hosts );

        controller = new Host( "localhost", 11111 );
        config = newConfig( "localhost", 11111 );

        for( int i = 0; i < hosts; ++i ) {

            Config config = newConfig( "localhost", Config.DEFAULT_PORT + i );
            configs.add( config );
            
            daemons.add( new FSDaemon( config ) );

        }

        log.info( "Working with configs %s and daemons %s" , configs, daemons );

    }

    public void tearDown() {

        for( FSDaemon daemon : daemons ) {
            daemon.shutdown();
        }

        super.tearDown();

    }

    public void test() throws Exception {

        int[] concurrencyTests = new int[] { 1, 2, 4, 8 };
        int[] replicaTests = new int[] { 1, 2, 3 };
        int[] hostTests = new int[] { 1, 2, 4, 8 };

        for( int concurrency : concurrencyTests ) {

            for( int replicas : replicaTests ) {

                for( int hosts : hostTests ) {

                    this.concurrency = concurrency;
                    this.replicas = replicas;
                    this.hosts = hosts;

                    try {
                        setUp();
                    } catch ( PartitionLayoutException e ) {
                        // this is just an invalid config so skip it.
                        continue;
                    }

                    doTest();
                    tearDown();
                    
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

}