package peregrine.pfsd.shuffler;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.pagerank.*;
import peregrine.io.partition.*;
import peregrine.pfsd.shuffler.*;

public class TestOutputBuffer extends peregrine.BaseTest {

    public void setUp() {

        Config.setHost( new Host( "localhost" ) );
        
        // TRY with three partitions... 
        Config.addPartitionMembership( 0, "localhost" );
        Config.addPartitionMembership( 1, "localhost" );

    }
    
    public void test1() throws IOException {

        OutputBuffer buff = new OutputBuffer( "/tmp/shuffle1.test" );

        int max_writes = 1000;
        int max_partitions = Config.getPartitionMembership().size();

        for( int i = 0; i < max_writes; ++i ) {
        
            for( int j = 0 ; j < max_partitions; ++j ) {

                buff.accept( j, j , new byte[2048] );
                
            }

        }

        buff.close();
        
    }

    public static void main( String[] args ) throws Exception {

        TestOutputBuffer test = new TestOutputBuffer();
        test.setUp();
        test.test1();
        test.tearDown();
        
    }

}