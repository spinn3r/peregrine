package peregrine;

import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.pfsd.*;

public class PFSTest extends peregrine.BaseTest {

    protected FSDaemon daemon = null;
    
    public void setUp() {

        super.setUp();

        Config config = new Config();

        config.setHost( new Host( "localhost", Config.DEFAULT_PORT ) );

        config.initEnabledFeatures();
        
        daemon = new FSDaemon( config );
        
    }

    public void tearDown() {

        daemon.shutdown();
        
        super.tearDown();

        //FIXME: remove this when I can shut down without having extra threads
        //lying around.  Rework log5j for this.
        //System.exit( 0 );

    }

}