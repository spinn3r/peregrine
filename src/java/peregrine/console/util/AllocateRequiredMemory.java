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
package peregrine.console.util;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.lang.reflect.*;

import com.spinn3r.log5j.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.controller.rpcd.delegate.*;
import peregrine.io.*;
import peregrine.os.*;
import peregrine.rpc.*;
import peregrine.util.*;
import peregrine.worker.*;

/**
 * Allocate ALL memory for the given number of daemons and the current config.
 */
public class AllocateRequiredMemory {

    public static void main( String[] args ) throws Exception {
        
        Getopt getopt = new Getopt( args );

        Config config = ConfigParser.parse( args );

        getopt.require( "daemons" );

        int daemons = getopt.getInt( "daemons" );

        System.out.printf( "Testing for %,d daemons\n", daemons );
        
        long capacity = 0;
        
        capacity += config.getMaxDirectMemory() * daemons;

        capacity += config.getMaxMemory() * daemons;

        List<Integer> capacities = new ArrayList();

        while( true ) {

            if ( capacity < 0 ) {
                break;
            } else if ( capacity < Integer.MAX_VALUE ) {
                capacities.add( (int)capacity );
                break;
            } else {
                capacities.add( Integer.MAX_VALUE );
                capacity -= Integer.MAX_VALUE;
            }
            
        }

        System.out.printf( "Allocating buffers of: %s\n", capacities );
        
        List<ByteBuffer> buffers = new ArrayList();

        long allocated = 0;
        
        for( int cap : capacities ) {
            ByteBuffer buff = ByteBuffer.allocateDirect( cap );
            buffers.add( buff );
            allocated += cap;
        }

        System.out.printf( "Allocated %,d bytes\n", allocated );

        //now actualy USE the allocated memory because we have to factor in
        //overcommit

        System.out.printf( "Using memory..." );
        
        for( ByteBuffer buff : buffers ) {
            
            for( long i = 0; i < buff.capacity(); ++i ) {
                buff.put( (byte)1 );
            }

        }

        System.out.printf( "done\n" );
        
    }

}