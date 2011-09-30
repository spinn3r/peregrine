package maprunner.test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.io.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.shuffle.*;
import maprunner.keys.*;

/**
 * 
 */
public class TestFullOuterJoin {

    public static void main( String[] args ) throws Exception {

        //write keys to two files but where there isn't a 100%
        //intersection... then try to join against these files. 

        Config.addPartitionMembership( 0, "cpu0" );

        //now test writing two regions to a file and see if both sides of the
        //join are applied correctly

        Partition part = new Partition( 0 );
        Host host = new Host( "cpu0", 0, 0 );
        
        PartitionWriter writer;

        writer = new PartitionWriter( part, "/tmp/left" );

        write( writer, 1 );
        write( writer, 2 );
        write( writer, 3 );
        write( writer, 4 );
        write( writer, 5 );

        writer.close();

        writer = new PartitionWriter( part, "/tmp/right" );

        write( writer, 4 );
        write( writer, 5 );
        write( writer, 6 );
        write( writer, 7 );
        write( writer, 8 );

        writer.close();

        int nr_files = 2;

        List<LocalPartitionReader> readers = new ArrayList();
        readers.add( new LocalPartitionReader( part, host, "/tmp/left" ) );
        readers.add( new LocalPartitionReader( part, host, "/tmp/right" ) );
        
        Joiner joiner = new Joiner( readers );

        while( true ) {

            JoinedTuple joined = joiner.next();

            if ( joined == null )
                break;

            System.out.printf( "FIXME: %s\n", Hex.encode( joined.key ) );
            
        }
        
    }

    public static void write( PartitionWriter writer,
                              int v ) throws IOException {

        byte[] key = new IntKey( v ).toBytes();
        byte[] value = key;
        
        writer.write( key, value );
    }

}

