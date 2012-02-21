/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.pfsd.shuffler;

import java.io.*;
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.shuffle.receiver.*;

import org.jboss.netty.buffer.*;

public class TestShuffler extends peregrine.BaseTest {

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

        ShuffleReceiverFactory factory = new ShuffleReceiverFactory( config );
        
        ShuffleReceiver shuffleReceiver = factory.getInstance( "default" );

        int max_writes = 1000;
        int max_partitions = config.getMembership().size();
        
        for( int i = 0; i < max_writes; ++i ) {
        
            for( int j = 0 ; j < max_partitions; ++j ) {

                int from_partition = i;
                int from_chunk = i;
                int to_partition = j;

                ChannelBuffer buff = ChannelBuffers.directBuffer( 2048 );
                
                shuffleReceiver.accept( from_partition, from_chunk, to_partition, 1, buff );

            }

        }

        shuffleReceiver.close();

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
