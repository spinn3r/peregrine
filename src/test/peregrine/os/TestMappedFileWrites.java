package peregrine.os;

import peregrine.*;
import peregrine.config.*;
import peregrine.config.partitioner.*;
import peregrine.util.netty.*;

import org.jboss.netty.buffer.*;

public class TestMappedFileWrites extends BaseTest {

	public void test1() throws Exception {

        Config config = new Config();
        config.init();
        config.initEnabledFeatures();

        System.out.printf( "%s\n", config.toDesc() );
        
        MappedFile mappedFile = new MappedFile( config, config.getBasedir() + "/test.dat", "w" );

        ChannelBufferWritable writable = mappedFile.getChannelBufferWritable();

        int size = 1024;
        int max = 10 * 1024;

        for( int i = 0; i < max; ++i ) {
            writable.write( ChannelBuffers.buffer( size ) );
        }
        
        // now write a ton of data.
        
	}
	
	public static void main(String[] args) throws Exception {

        runTests();
		
	}

}
