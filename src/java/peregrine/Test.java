package peregrine;

import java.io.*;
import java.util.*;
import java.net.*;
import java.nio.*;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.util.*;
import peregrine.io.chunk.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.shuffle.*;
import peregrine.reduce.sorter.*;
import peregrine.task.*;
import peregrine.pfs.*;
import peregrine.config.*;

public class Test {

    public static void test0() throws Exception {

        System.gc();

        System.out.printf( "-------- test ChannelBuffers.buffer\n" );

        long before = System.currentTimeMillis();

        int max = 100;

        int capacity = 1000000; 

        System.out.printf( "using capacity: %s\n", capacity );

        List<ChannelBuffer> list = new ArrayList();

        for ( int i = 0; i < max; ++i ) {
            ChannelBuffer buff = ChannelBuffers.buffer( capacity );
            list.add( buff );
        }

        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d\n", (after-before) );

    }

    public static void test1() throws Exception {

        System.gc();

        System.out.printf( "-------- test ChannelBuffers.directBuffer\n" );

        long before = System.currentTimeMillis();

        int max = 10000;

        int capacity = 16384; 

        System.out.printf( "using capacity: %s\n", capacity );

        List<ChannelBuffer> list = new ArrayList();

        for ( int i = 0; i < max; ++i ) {
            ChannelBuffer buff = ChannelBuffers.directBuffer( capacity );
            list.add( buff );
        }

        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d\n", (after-before) );

    }

    public static void main( String[] args ) throws Exception {

        test0();
        test0();
        test0();
        test0();

        test1();
        test1();
        test1();
        test1();

        
    }

}
