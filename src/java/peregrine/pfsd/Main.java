package peregrine.pfsd;

import java.io.*;
import java.util.*;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class Main {

    public static int PORT = 11112;

    public static void main(String[] args) {

        // first configure netty to use log4j 
        InternalLoggerFactory.setDefaultFactory( new Log4JLoggerFactory() );

        // now init log4j ... 

        org.apache.log4j.xml.DOMConfigurator.configure( "conf/log4j.xml" );

        // Configure the server.
        ServerBootstrap bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new FSPipelineFactory() );

        // Bind and start to accept incoming connections.
        bootstrap.bind( new InetSocketAddress( PORT ) );

    }

}
    