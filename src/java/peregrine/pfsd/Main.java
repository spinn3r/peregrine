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

        // init log4j ... 
        org.apache.log4j.xml.DOMConfigurator.configure( "conf/log4j.xml" );

        Properties props = new Properties();
        props.load( new FileInputStream( "conf/peregrine.conf" ) );

        String root = props.get( "root" ).toString();
        int port    = Integer.parseInt( props.get( "port" ).toString() );

        if ( args.length == 2 ) {

            root = args[0];
            port = Integer.parseInt( args[1] );
            
        }

        String hostname = System.getenv( "HOSTNAME" );

        if ( hostname == null )
            hostname = "localhost";
        
        log.info( "Starting on %s on port %s" , hostname, port );
        
        Config config = new Config();

        config.setRoot( root );
        config.setHost( new Host( hostname, port ) );
        
        new FSDaemon( config );

        Thread.sleep( Long.MAX_VALUE );
        
    }

}
    