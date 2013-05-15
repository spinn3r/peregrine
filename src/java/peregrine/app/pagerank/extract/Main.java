/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine.app.pagerank.extract;

import peregrine.*;
import peregrine.app.pagerank.extract.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.rpc.*;
import peregrine.util.*;
import peregrine.worker.*;

public class Main {

    public static void main( String[] args ) throws Exception {

        Config config = ConfigParser.parse( args );
        new Initializer( config ).basic( Main.class );

        Getopt getopt = new Getopt( args );

        getopt.require( "path" );
        
        String path               = getopt.getString( "path" );
        String graph              = getopt.getString( "graph", "/pr/graph" );
        String nodes_by_hashcode  = getopt.getString( "nodes_by_hashcode", "/pr/nodes_by_hashcode" );
        boolean caseInsensitive   = getopt.getBoolean( "caseInsensitive" );
        int maxChunks             = getopt.getInt( "maxChunks", Integer.MAX_VALUE );

        Extract extract = new Extract( path, graph, nodes_by_hashcode, caseInsensitive );
        extract.setMaxChunks( maxChunks );
        extract.prepare();
        extract.init( args );
        
        ControllerClient.submit( config, extract );

    }

}
