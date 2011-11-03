package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.keys.*;
import peregrine.map.*;
import peregrine.reduce.*;
import peregrine.util.*;

/**
 * 
 */
public class TestFullOuterJoin extends peregrine.BaseTestWithTwoPartitions {

    public void test1() throws Exception {

        //write keys to two files but where there isn't a 100%
        //intersection... then try to join against these files. 

        //now test writing two regions to a file and see if both sides of the
        //join are applied correctly

        Partition part = new Partition( 0 );
        
        PartitionWriter writer;

        writer = new DefaultPartitionWriter( config, part, "/tmp/left" );

        write( writer, 1 );
        write( writer, 2 );
        write( writer, 3 );
        write( writer, 4 );
        write( writer, 5 );

        writer.close();

        writer = new DefaultPartitionWriter( config, part, "/tmp/right" );

        write( writer, 4 );
        write( writer, 5 );
        write( writer, 6 );
        write( writer, 7 );
        write( writer, 8 );

        writer.close();

        int nr_files = 2;

        List<LocalPartitionReader> readers = new ArrayList();
        
        readers.add( new LocalPartitionReader( config, part, "/tmp/left" ) );
        readers.add( new LocalPartitionReader( config, part, "/tmp/right" ) );
        
        LocalMerger merger = new LocalMerger( readers );

        //FIXME: make sure the results come back ordered correctly... 
        
        while( true ) {

            JoinedTuple joined = merger.next();

            if ( joined == null )
                break;

            System.out.printf( "joined: %s, left=%s, right=%s\n",
                               Hex.encode( joined.key ), Hex.encode( joined.values[0] ), Hex.encode( joined.values[1] ) );
            
        }

    }

    public static void write( PartitionWriter writer,
                              int v ) throws IOException {

        byte[] key = new IntKey( v ).toBytes();
        byte[] value = key;
        
        writer.write( key, value );
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}

