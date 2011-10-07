package peregrine.pfsd;

import java.io.*;
import java.util.*;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class Main {

    public static void main(String[] args) {

        // init log4j ... 
        org.apache.log4j.xml.DOMConfigurator.configure( "conf/log4j.xml" );

        new FSDaemon();
        
    }

}
    