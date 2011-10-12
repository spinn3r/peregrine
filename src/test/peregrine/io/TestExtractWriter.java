package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.shuffle.*;
import peregrine.io.*;

import peregrine.io.partition.*;
import peregrine.io.chunk.*;

public class TestExtractWriter extends peregrine.BaseTest {

    protected Config config;
    
    public void setUp() {

        super.setUp();

        config = new Config();
        
        config.setHost( new Host( "localhost" ) );

        //PartitionWriter 
        config.addPartitionMembership( 0, "localhost" );
        config.addPartitionMembership( 1, "localhost" );

    }
    
    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        String path = "/test/extract1";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        for ( int i = 0; i < 100; ++i ) {

            byte[] key = new StructWriter()
                .writeVarint( i )
                .toBytes()
                ;

            byte[] value = key;

            writer.write( key, value );
            
        }

        writer.close();

        System.out.printf( "worked.\n" );

    }

    public static void main( String[] args ) throws Exception {

        TestExtractWriter t = new TestExtractWriter();
        //System.out.printf( "%s\n", t.run() );

        t.setUp();
        t.test1();
        //t.tearDown();
        
        //t.test1();
        
    }

}
