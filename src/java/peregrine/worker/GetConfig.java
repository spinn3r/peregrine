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
import peregrine.util.*;

import com.spinn3r.log5j.Logger;

/**
 * Parse the config and return the correct amount of memory to constrain the VM.
 * The given command line arguments are passed to ConfigParser and applied with
 * the config.
 */
public class GetConfig {
	
    private static final Logger log = Logger.getLogger();

    public static void main( String[] args ) throws Exception {

        Getopt getopt = new Getopt( args );
        
        Config config = ConfigParser.parse( args );

        Map<String,String> dict = config.toDict();

        // read a specific key.  In the future we should support getInt, etc. 
        if ( getopt.containsKey( "getSize" ) ) {

            String key = getopt.getString( "getSize" );
            System.out.printf( "%s\n", config.getStructMap().getSize( key ) );

        } else if ( getopt.containsKey( "getString" ) ) {

            String key = getopt.getString( "getString" );
            System.out.printf( "%s\n", config.getStructMap().getString( key ) );

        } else {
            
            for( String key : dict.keySet() ) {
                System.out.printf( "%s=%s\n", key, dict.get( key ) );
            }

        }
        
    }
    
}
    
