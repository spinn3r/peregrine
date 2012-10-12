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
package peregrine.worker;

import java.io.*;
import java.util.*;

import peregrine.config.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 * Parse the config and return the correct amount of memory to constrain the VM.
 */
public class GetMaxDirectMemory {
	
    private static final Logger log = Logger.getLogger();

    public static void main( String[] args ) throws Exception {

        Config config = ConfigParser.parse( args );

        long maxDirectMemorySize =
            //the concurrency essentially 
            config.getConcurrency() *
            // only one of these variables can be active at one time.
            Math.max( config.getShuffleBufferSize(), config.getSortBufferSize() );

        System.out.printf( "%s\n", maxDirectMemorySize );

    }
    
}
    
