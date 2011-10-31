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
import peregrine.reduce.*;
import peregrine.io.*;
import peregrine.io.async.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;
import peregrine.pfs.*;
import peregrine.pfsd.*;

public class BaseTestWithTwoDaemons extends peregrine.BaseTest {

    protected Host controller;

    protected Config config;

    protected Config config0;
    protected Config config1;

    protected List<FSDaemon> daemons = new ArrayList();
    
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