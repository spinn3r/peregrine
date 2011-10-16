package peregrine;

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
import peregrine.pfsd.*;

public class TestMapReduce extends peregrine.BaseTestWithTwoDaemons {

    public static class Map extends Mapper {

        @Override
        public void map( byte[] key,
                         byte[] value ) {

            emit( key, value );
            
        }

    }

    public static class Reduce extends Reducer {

        int count = 0;

        int nth = 0;
        
        @Override
        public void reduce( byte[] key, List<byte[]> values ) {

            ++count;

            List<Integer> ints = new ArrayList();

            for( byte[] val : values ) {
                ints.add( IntBytes.toInt( val ) );
            }
            
            // full of fail... 
            if ( values.size() != 2 )
                throw new RuntimeException( String.format( "%s does not equal %s (%s) on nth reduce %s" , values.size(), 2, ints, nth ) );

            ++nth;
            
        }

        @Override
        public void cleanup() {

            if ( count == 0 )
                throw new RuntimeException();
            
        }

    }

    public void test1() throws Exception {

        String path = String.format( "/test/%s/test1.in", getClass().getName() );
        
        ExtractWriter writer = new ExtractWriter( config, path );

        int max = 1000;
        
        for( int i = 0; i < max; ++i ) {

            byte[] key = new IntKey( i ).toBytes();
            byte[] value = key;
            writer.write( key, value );
            
        }

        for( int i = 0; i < max; ++i ) {

            byte[] key = new IntKey( i ).toBytes();
            byte[] value = key;
            writer.write( key, value );
            
        }

        writer.close();

        // int count = 0;
        
        // //make sure too many values weren't written.
        // Membership membership = config.getPartitionMembership();

        // for( Partition part : membership.getPartitions() ) {

        //     for( Host host : membership.getHosts( part ) ) {

        //         LocalPartitionReader reader = new LocalPartitionReader( config, part, host, path );

        //         while( reader.hasNext() ) {

        //             byte[] key = reader.key();
        //             byte[] value = reader.value();

        //             ++count ;
                    
        //         }
                
        //     }
            
        // }

        // //FIXME: read the value off BOTH partitions / hosts.
        // assertTrue( count > 0 );
        
        String output = String.format( "/test/%s/test1.out", getClass().getName() );

        Controller controller = new Controller( config );
        
        controller.map( Map.class, path );
        controller.reduce( Reduce.class, null, new Output( output ) );

        controller.shutdown();
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}