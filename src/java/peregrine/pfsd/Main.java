package peregrine.pfsd;

import java.io.*;
import java.util.*;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import peregrine.*;

public class Main {

    public static void main(String[] args) throws Exception {

        // init log4j ... 
        org.apache.log4j.xml.DOMConfigurator.configure( "conf/log4j.xml" );

        String dir = Config.PFS_ROOT;
        int port = FSDaemon.PORT;

        if ( args.length == 2 ) {

            dir = args[0];
            port = Integer.parseInt( args[1] );
            
        }

        new FSDaemon( dir , port );

        Thread.sleep( Long.MAX_VALUE );
        
    }

}
    