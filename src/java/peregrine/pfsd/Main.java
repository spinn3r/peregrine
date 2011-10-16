package peregrine.pfsd;

import java.io.*;
import java.util.*;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import peregrine.*;

import com.spinn3r.log5j.Logger;

public class Main {

    private static final Logger log = Logger.getLogger();

    public static void main(String[] args) throws Exception {

        org.apache.log4j.xml.DOMConfigurator.configure( "conf/log4j.xml" );

        Config config = Config.parse( new File( "conf/peregrine.conf" ) );
        
        if ( args.length == 2 ) {

            config.setRoot( args[0] );
            config.getHost().setPort( Integer.parseInt( args[1] ) );
            
        }

        log.info( "Starting on %s with controller: %s" , config.getHost(), config.getController() );

        new FSDaemon( config );

        Thread.sleep( Long.MAX_VALUE );
        
    }

}
    