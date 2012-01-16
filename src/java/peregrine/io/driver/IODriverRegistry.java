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
package peregrine.io.driver;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.config.*;
import peregrine.io.driver.*;
import peregrine.io.partition.*;
import peregrine.shuffle.sender.*;

import peregrine.io.driver.blackhole.*;
import peregrine.io.driver.shuffle.*;
import peregrine.io.driver.file.*;

/**
 * Keeps track of URI schemes and registered drivers.
 * 
 */
public class IODriverRegistry {
	
	//TODO: this should probably be moved to some sort of bootstrap for the 
	//entire peregrine system so that we have no global state. 

    public static Map<String,IODriver> registry = new ConcurrentHashMap();
    
    public static void register( IODriver driver ) {
        registry.put( driver.getScheme(), driver );
    }

    public static IODriver getInstance( String scheme ) {
        return registry.get( scheme );
    }

    static {

        // register all internal drivers
        register( new BlackholeIODriver() );
        register( new ShuffleIODriver() );
        register( new FileIODriver() );
        
    }
    
}
