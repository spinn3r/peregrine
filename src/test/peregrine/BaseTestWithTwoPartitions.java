package peregrine;

import java.util.*;
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.pfsd.*;

public class BaseTestWithTwoPartitions extends peregrine.BaseTest {

    protected Config config;

    protected List<FSDaemon> daemons = new ArrayList();
    
    public void setUp() {

        super.setUp();

        config = new Config();
        config.setHost( new Host( "localhost" ) );

        config.setConcurrency( 2 );
        
        // TRY with three partitions... 
        config.getHosts().add( new Host( "localhost" ) );

        config.init();

        daemons.add( new FSDaemon( config ) );

    }

    public void tearDown() {

        for( FSDaemon daemon : daemons ) {
            daemon.shutdown();
        }
        
        super.tearDown();

    }

}