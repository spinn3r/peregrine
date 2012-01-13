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
package peregrine.app.pagerank;

import peregrine.*;
import peregrine.config.Config;
import peregrine.controller.*;
import peregrine.io.*;

import com.spinn3r.log5j.Logger;

public class Pagerank {

    private static final Logger log = Logger.getLogger();

    private Config config;

    /**
     * The number of iterations we should perform.
     */
    private int iterations = 5;
    
    public Pagerank( Config config ) {
        this.config = config;
    }
    
    public void exec( String path ) throws Exception {

        Controller controller = new Controller( config );

        try {

            // ***** INIT stage... 

            // TODO: We can elide this and the next step by reading the input
            // once and writing two two destinations.  this would read from
            // 'path' and then wrote to node_indegree and graph_by_source at the
            // same time.

            // ***
            //
            // compute the node_indegree 

            controller.map( NodeIndegreeJob.Map.class,
                            new Input( path ),
                            new Output( "shuffle:default" ) );

            controller.reduce( NodeIndegreeJob.Reduce.class,
                               new Input( "shuffle:default" ),
                               new Output( "/pr/tmp/node_indegree" ) );

            // ***
            //
            // sort the graph by source since we aren't certain to have have the
            // keys in the right order and store in graph_by_source for joining
            // across every iteration.  This is invariant so we should store it
            // to the filesystem.
            // 

            controller.map( Mapper.class,
                            new Input( path ),
                            new Output( "shuffle:default" ) );
            
            controller.reduce( Reducer.class,
                               new Input( "shuffle:default" ),
                               new Output( "/pr/test.graph_by_source" ) );
            
            // ***
            //
            // now create node metadata...  This will write the dangling vector,
            // the nonlinked vector and node_metadata which are all invariant.

            controller.merge( NodeMetadataJob.Map.class,
                              new Input( "/pr/tmp/node_indegree",
                                         "/pr/test.graph_by_source" ),
                              new Output( "/pr/out/node_metadata" ,
                                          "/pr/out/dangling" ,
                                          "/pr/out/nonlinked" ,
                                          "broadcast:nr_nodes" ,
                                          "broadcast:nr_dangling" ) );

            controller.reduce( NodeMetadataJob.Reduce.class,
                               new Input( "shuffle:nr_nodes" ),
                               new Output( "/pr/out/nr_nodes" ) );

            controller.reduce( NodeMetadataJob.Reduce.class,
                               new Input( "shuffle:nr_dangling" ),
                               new Output( "/pr/out/nr_dangling" ) );

            // init the empty rank_vector table ... we need to merge against it.

            // ***** ITER stage... 

            for( int iter = 0; iter < iterations; ++iter ) {

                if ( iter == 0 ) {

                    // init empty files which we can still join against.
                    
                    controller.map( Mapper.class,
                                    new Input(),
                                    new Output( "/pr/out/rank_vector" ) );
                    
                    controller.map( Mapper.class,
                                    new Input(),
                                    new Output( "/pr/out/teleportation_grant" ) );

                }

                controller.merge( IterJob.Map.class,
                                  new Input( "/pr/test.graph_by_source" ,
                                             "/pr/out/rank_vector" ,
                                             "/pr/out/dangling" ,
                                             "/pr/out/nonlinked" ,
                                             "broadcast:/pr/out/nr_nodes" ) ,
                                  new Output( "shuffle:default",
                                              "broadcast:dangling_rank_sum" ) );

                // ***
                // 
                // write out the new ranking vector

                if ( iter < iterations - 1 ) {
                
                    controller.reduce( IterJob.Reduce.class,
                                       new Input( "shuffle:default",
                                                  "broadcast:/pr/out/nr_nodes",
                                                  "broadcast:/pr/out/nr_dangling" ),
                                       new Output( "/pr/out/rank_vector" ) );

                    // now compute the dangling rank sum for the next iteration

                    controller.reduce( TeleportationGrantJob.Reduce.class, 
                                       new Input( "shuffle:dangling_rank_sum",
                                                  "broadcast:/pr/out/nr_nodes" ),
                                       new Output( "/pr/out/teleportation_grant" ) );

                }
                    
            }

            log.info( "Pagerank complete" );
            
        } finally {

            // Shutdown the controller and release all resources.  Note that
            // this must be done in a finally block so that we don't leave the
            // cluster in an inconsistent state.
            
            controller.shutdown();
            
        }
            
    }

}
