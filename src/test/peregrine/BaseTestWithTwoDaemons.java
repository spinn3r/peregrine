package peregrine;

import java.util.*;
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.pfsd.*;

public abstract class BaseTestWithTwoDaemons extends peregrine.BaseTest {

    protected Host controller;

    protected Config config;

    protected Config config0;
    protected Config config1;

    protected List<FSDaemon> daemons = new ArrayList();

    private int concurrency;
    private int replicas;
    
    public BaseTestWithTwoDaemons() {
        this( 1, 1 );
    }

    public BaseTestWithTwoDaemons( int concurrency, int replicas ) {
        this.concurrency = concurrency;
        this.replicas = replicas;
    }

    public void setUp() {

        super.setUp();

        controller = new Host( "localhost", 11111 );

        config = newConfig( "localhost", 11111 );
        
        config0 = newConfig( "localhost", 11112 );
        config1 = newConfig( "localhost", 11113 );

        daemons.add( new FSDaemon( config0 ) );
        daemons.add( new FSDaemon( config1 ) );

    }

    protected Config newConfig( String host, int port ) {

        Config config = new Config( host, port );

        config.setController( controller );
        config.setConcurrency( concurrency );
        config.setReplicas( replicas );
        
        config.getHosts().add( new Host( "localhost", 11112 ) );
        config.getHosts().add( new Host( "localhost", 11113 ) );
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