package peregrine;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.shuffle.*;
import peregrine.io.*;
import peregrine.io.async.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;
import peregrine.pfs.*;
import peregrine.pfsd.*;

public class BaseTestWithTwoDaemons extends peregrine.BaseTest {

    protected Config config;
    protected Config config0;
    protected Config config1;

    protected List<FSDaemon> daemons = new ArrayList();
    
    public void setUp() {

        super.setUp();
        
        config0 = newConfig( "localhost", 11112 );
        config1 = newConfig( "localhost", 11113 );

        daemons.add( new FSDaemon( config0 ) );
        daemons.add( new FSDaemon( config1 ) );

        config = config0;
        
    }

    protected Config newConfig( String host, int port ) {

        Config config = new Config( host, port );

        config.addPartitionMembership( 0, new Host( "localhost", 11112 ) );
        config.addPartitionMembership( 1, new Host( "localhost", 11113 ) );

        return config;
        
    }

    public void tearDown() {

        for( FSDaemon daemon : daemons ) {
            daemon.shutdown();
        }
        
        super.tearDown();

    }

}