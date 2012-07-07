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
package peregrine.controller;

import java.io.*;
import java.util.*;

import peregrine.config.*;
import peregrine.os.*;
import peregrine.worker.*;

import com.spinn3r.log5j.Logger;

/**
 * Command line main() class for the controller daemon.
 */
public class Main {
	
    private static final Logger log = Logger.getLogger();

    public static void main( String[] args ) throws Exception {

        Config config = ConfigParser.parse( args );

        Initializer init = new Initializer( config );
        init.controller();

        Controller controller = new Controller( config );

        Thread.sleep( Long.MAX_VALUE );
        
    }
        
}
    
