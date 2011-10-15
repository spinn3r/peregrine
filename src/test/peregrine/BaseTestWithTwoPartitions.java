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

public class BaseTestWithTwoPartitions extends peregrine.BaseTest {

    protected Config config;

    protected List<FSDaemon> daemons = new ArrayList();
    
    public void setUp() {

        super.setUp();
        
        config = newConfig( "localhost", 11112 );
        daemons.add( new FSDaemon( config ) );

    }

    private Config newConfig( String host, int port ) {

        Config config = new Config( host, port );

        config.addPartitionMembership( 0, new Host( "localhost", 11112 ) );
        config.addPartitionMembership( 1, new Host( "localhost", 11112 ) );

        return config;
        
    }

    public void tearDown() {

        for( FSDaemon daemon : daemons ) {
            daemon.shutdown();
        }
        
        super.tearDown();

    }

}