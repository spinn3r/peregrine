package peregrine.pfsd.shuffler;

import java.io.*;
import java.util.*;
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.config.Partition;
import peregrine.util.*;
import peregrine.shuffle.*;

import org.jboss.netty.buffer.*;

public class TestShuffleOutputWriter extends peregrine.BaseTest {

    protected Config config;
    public void setUp() {

        super.setUp();
        
        config = new Config();
        config.setHost( new Host( "localhost" ) );

        config.setConcurrency( 2 );
        
        // TRY with three partitions... 
        config.getHosts().add( new Host( "localhost" ) );

        config.init();
        
    }
    
    public void test1() throws IOException {

        String path = "/tmp/shuffle1.test";
        
        ShuffleOutputWriter buff = new ShuffleOutputWriter( config, path );

        int max_writes = 10;
        int max_partitions = config.getMembership().size();

        for( int i = 0; i < max_writes; ++i ) {
        
            for( int j = 0 ; j < max_partitions; ++j ) {

                int from_partition = i;
                int from_chunk = i;
                int to_partition = j;

                byte[] bytes = new byte[] { (byte)6, (byte)7, (byte)8, (byte)9 };

                ChannelBuffer value = ChannelBuffers.directBuffer( bytes.length );
                value.writeBytes( bytes );

                buff.accept( from_partition, from_chunk, to_partition, 1, value );
                
            }

        }

        buff.close();

        System.out.printf( "Reading from path: %s\n", path );

        List<Partition> partitions = new ArrayList();
        partitions.add( new Partition( 1 ) ) ;

        ShuffleInputReader reader = new ShuffleInputReader( null, path, partitions );

        int count = 0;
        while( reader.hasNext() ) {

            ShufflePacket pack = reader.next();

            System.out.printf( "from_partition: %s, from_chunk: %s, to_partition: %s, data length: %,d, data: %s \n",
                               pack.from_partition, pack.from_chunk, pack.to_partition, pack.data.capacity(), Hex.encode( pack.data ) );
            
            assertEquals( pack.to_partition, 1 );
            
            ++count;

        }
        
        assertEquals( max_writes, count );

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}