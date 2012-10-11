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
package peregrine.app.flow;

import peregrine.app.pagerank.extract.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.util.*;
import peregrine.worker.*;

/**
 * 
 */
public class Main {

    public static void main( String[] args ) throws Exception {

        Config config = ConfigParser.parse( args );
        new Initializer( config ).basic( Main.class );

        Getopt getopt = new Getopt( args );

        getopt.require( "input", "output", "sources" );
        
        String input             = getopt.getString( "input" );
        String output            = getopt.getString( "output" ); 
        String sources           = getopt.getString( "sources" );
        int iterations           = getopt.getInt( "iterations", 5 );
        boolean caseInsensitive  = getopt.getBoolean( "caseInsensitive" );
        
        Flow flow = new Flow( input, output, sources, iterations, caseInsensitive );
        flow.init( args );
        
        ControllerClient.submit( config, flow );
        
    }

}
