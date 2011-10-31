package peregrine.pfsd;

import java.io.*;
import java.util.*;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.socket.nio.*;

import peregrine.*;
import peregrine.config.*;

import org.apache.log4j.xml.DOMConfigurator;

import com.spinn3r.log5j.Logger;

public class Main {

    private static final Logger log = Logger.getLogger();

    public static void main(String[] args ) throws Exception {

        DOMConfigurator.configure( "conf/log4j.xml" );
        Config config = ConfigParser.parse( args );

        log.info( "Starting on %s with controller: %s" , config.getHost(), config.getController() );

        new FSDaemon( config );

        Thread.sleep( Long.MAX_VALUE );
        
    }

}
    