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
package peregrine.os;

import peregrine.*;
import peregrine.config.*;
import peregrine.config.partitioner.*;
import peregrine.util.netty.*;
import peregrine.io.util.*;

import org.jboss.netty.buffer.*;

public class TestMappedFileWrites extends BaseTest {

	public void test1() throws Exception {

        Config config = new Config();

        Files.mkdirs( config.getBasedir() );

        //config.init();
        config.initEnabledFeatures();

        System.out.printf( "%s\n", config.toDesc() );
        
        ChannelBufferWritable writable = new MappedFileWriter( config, config.getBasedir() + "/test.dat" );

        int size = 1024;
        int max = 200 * 1024;

        for( int i = 0; i < max; ++i ) {
            writable.write( ChannelBuffers.wrappedBuffer( new byte[size] ) );
        }
        
        // now write a ton of data.

        writable.sync();
        writable.close();
        
	}
	
	public static void main(String[] args) throws Exception {

        runTests();
		
	}

}
