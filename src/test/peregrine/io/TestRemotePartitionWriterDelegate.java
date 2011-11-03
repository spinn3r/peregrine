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
import peregrine.reduce.*;
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.async.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;
import peregrine.pfsd.*;

public class TestRemotePartitionWriterDelegate extends peregrine.PFSTest {

    protected Config config;
    
    public void setUp() {

        super.setUp();
        
        config = new Config();
        
        config.setHost( new Host( "localhost" ) );

    }
        
    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        Partition part = new Partition( 0 );
        String path = "/test/remote-write1" ;
        
        RemotePartitionWriterDelegate delegate = new RemotePartitionWriterDelegate();
        delegate.init( config, part, config.getHost(), path );

        int chunk_id = 0;
        
        ChunkWriter writer = new DefaultChunkWriter( delegate.newChunkWriter( chunk_id ) );

        int max = 1000;

        int computed_written = 0;
        
        for( int i = 0; i < max; ++i ) {

            byte[] key = new StructWriter()
                .writeVarint( i )
                .toBytes()
                ;

            byte[] value = key;

            writer.write( key, value );

            computed_written += key.length + value.length + 2 ;

            assertEquals( computed_written, writer.length() );

        }

        writer.close();

        long length = writer.length();
        System.out.printf( "Wrote %,d bytes.\n", length );

        // now use the chunk reader to find out what was written.

        File chunk = LocalPartition.getChunkFile( config, part, path, chunk_id );

        assertEquals( length, chunk.length() );

        System.out.printf( "Going to read from: %s\n", chunk );

        DefaultChunkReader reader = new DefaultChunkReader( chunk );

        int count = 0;

        while( reader.hasNext() ) {

            try {
                
                byte[] key = reader.key();
                byte[] value = reader.value();
                
                ++count;

            } catch ( Exception e ) {

                System.out.printf( "Failed to index %,d\n", count );
                
                throw e;
                
            }

        }

        assertEquals( count, max );

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
