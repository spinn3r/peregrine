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

public class Main {

    public static void main( String[] args ) throws Exception {

        Getopt getopt = new Getopt( args );

        Config config = ConfigParser.parse( args );
        new Initializer( config ).controller();

        String nodes_path = getopt.getString( "nodes_path" );
        String links_path = getopt.getString( "links_path" );
        
        new Wikirank( config, nodes_path, links_path ).exec();
        
    }

}
