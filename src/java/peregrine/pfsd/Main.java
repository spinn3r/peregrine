package peregrine.pfsd;

import java.io.*;
import java.util.*;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class Main {

    public static int PORT = 11112;
    
    public static void main(String[] args) {

        // Configure the server.
        ServerBootstrap bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory( new NettyPipelineFactory() );

        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress( PORT ));

    }

}
    