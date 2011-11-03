package peregrine;

import java.util.*;
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.pfsd.*;

import com.spinn3r.log5j.Logger;

public abstract class BaseTestWithMultipleDaemons extends peregrine.BaseTest {

    private static final Logger log = Logger.getLogger();

    protected Host controller;

    protected Config config;

    protected List<FSDaemon> daemons = new ArrayList();

    protected List<Config> configs = new ArrayList();

    private int concurrency;
    private int replicas;
    private int nr_daemons;
    
    public BaseTestWithMultipleDaemons() {
        this( 1, 1, 2 );
    }

    public BaseTestWithMultipleDaemons( int concurrency,
                                        int replicas,
                                        int nr_daemons ) {
        this.concurrency = concurrency;
        this.replicas = replicas;
        this.nr_daemons = nr_daemons;
    }

    public void setUp() {

        super.setUp();

        controller = new Host( "localhost", 11111 );
        config = newConfig( "localhost", 11111 );

        for( int i = 0; i < nr_daemons; ++i ) {

            Config config = newConfig( "localhost", Config.DEFAULT_PORT + i );
            configs.add( config );
            
            daemons.add( new FSDaemon( config ) );

        }

        log.info( "Working with configs %s and daemons %s" , configs, daemons );
        
    }

    protected Config newConfig( String host, int port ) {

        Config config = new Config( host, port );

        config.setController( controller );
        config.setConcurrency( concurrency );
        config.setReplicas( replicas );

        for( int i = 0; i < nr_daemons; ++i ) {
            config.getHosts().add( new Host( "localhost", Config.DEFAULT_PORT + i ) );
        }
        
        config.init();
        
        return config;
        
    }

    public void tearDown() {

        for( FSDaemon daemon : daemons ) {
            daemon.shutdown();
        }
        
        super.tearDown();

    }

}