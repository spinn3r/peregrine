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
        new Initializer( config ).controller();

        Getopt getopt = new Getopt( args );

        String path = getopt.getString( "path" );

        Controller controller = null;

        try {

            controller = new Controller( config );
            
            Message parameters = new Message();
            parameters.put( "path", path );
            
            controller.map( new Job().setDelegate( CorpusExtractJob.Map.class ) 
                                     .setInput( new Input( "blackhole:" ) )
                                     .setOutput( new Output( "shuffle:nodes", "shuffle:links" ) )
                                     .setParameters( parameters ) );
           
            controller.reduce( UniqueNodeJob.Reduce.class,
                               new Input( "shuffle:nodes" ),
                               new Output( "/pr/nodes_by_hashcode" ) );

            controller.reduce( MergeGraphJob.Reduce.class,
                               new Input( "shuffle:links" ),
                               new Output( "/pr/graph" ) );

        } finally {
            controller.shutdown();
        }
        
    }

}
