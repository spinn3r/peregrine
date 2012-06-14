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
package peregrine.app.wikirank;

import peregrine.io.*;
import peregrine.config.*;
import peregrine.util.*;
import peregrine.worker.*;
import peregrine.controller.*;

/**
 * Main binary for running peregrine on wikipedia graph data.
 *
 * ./bin/jexec peregrine.app.wikirank.Main --nodes_path=/d0/enwiki-20120502-page.sql --links_path=/d0/enwiki-20120502-pagelinks.sql
 */
public class Main {

    public static void main( String[] args ) throws Exception {

        Getopt getopt = new Getopt( args );

        Config config = ConfigParser.parse( args );
        new Initializer( config ).controller();

        String nodes_path = getopt.getString( "nodes_path" );
        String links_path = getopt.getString( "links_path" );

        String stage = getopt.getString( "stage", "run" );

        Controller controller = null;

        int result = 0;
        
        try {

            controller = new Controller( config );
            
            Wikirank wikirank = new Wikirank( config, controller, nodes_path, links_path );
            
            // extract , transform, load
            
            if ( "extract".equals( stage ) ) {
                wikirank.extract();
            } else if ( "fixup".equals( stage ) ) {
                wikirank.fixup();
            } else if ( "transform".equals( stage ) ) {
                wikirank.transform();
            } else if ( "load".equals( stage ) ) {
                wikirank.load();
            } else if ( "run".equals( stage ) ) {
                wikirank.run();
            } else {
                System.err.printf( "Unknown stage: %s\n", stage );
                result = 1;
            }

        } finally {
            controller.shutdown();
        }

        System.exit( result );
        
    }

}
