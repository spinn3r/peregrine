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

public class PFSTest extends peregrine.BaseTest {

    protected FSDaemon daemon = null;
    
    public void setUp() {

        super.setUp();

        Config config = new Config();

        config.setHost( new Host( "localhost", Config.DEFAULT_PORT ) );
        
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