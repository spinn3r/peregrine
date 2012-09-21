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

import java.io.*;

import peregrine.*;
import peregrine.config.Config;
import peregrine.controller.*;
import peregrine.io.*;

import com.spinn3r.log5j.Logger;

/**
 * <p> Pagerank implementation which uses Peregrine as a backend for fast
 * computation.
 *
 * <p>
 * Our pagerank implementation takes an input file and writes resulting files to
 * /pr/out/ for external analysis and use within external systems.
 */
public class Pagerank {
    
    private static final Logger log = Logger.getLogger();

    private Config config;

    /**
     */
    private String graph = null;

    /**
     */
    private String nodes_by_hashcode = null;
    
    /**
     * The number of iterations we should perform.
     */
    private int iterations = 5;

    private Controller controller = null;

    /**
     */
    private int step = 0;
    
    public Pagerank( Config config, String graph, String nodes_by_hashcode ) {
        this( config, graph, nodes_by_hashcode, new Controller( config ) );
    }

    public Pagerank( Config config, String graph, String nodes_by_hashcode, Controller controller ) {
        this.config = config;
        this.graph = graph;
        this.nodes_by_hashcode = nodes_by_hashcode;
        this.controller = controller;
    }

    /**
     * Init PR ... setup all our vectors, node metadata table, etc.
     */
    public void init() throws Exception {

        // ***** INIT stage... 

        log.info( "Running init() stage." );
        
        // TODO: We can elide this and the next step by reading the input
        // once and writing two two destinations.  this would read from
        // 'graph' and then wrote to node_indegree and graph_by_source at the
        // same time.

        // ***
        //
        // compute the node_indegree 

        controller.map( NodeIndegreeJob.Map.class,
                        new Input( graph ),
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
                        new Input( graph ),
                        new Output( "shuffle:default" ) );
        
        controller.reduce( GraphBySourceJob.Reduce.class,
                           new Input( "shuffle:default" ),
                           new Output( "/pr/graph_by_source" ) );
        
        // ***
        //
        // now create node metadata...  This will write the dangling vector,
        // the nonlinked vector and node_metadata which are all invariant.

        controller.merge( NodeMetadataJob.Map.class,
                          new Input( "/pr/tmp/node_indegree",
                                     "/pr/graph_by_source" ),
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

    }

    /**
     * Run one pagerank step.
     */
    public void iter() throws Exception {

        log.info( "Running init() stage (step=%s)", step );

        if ( step == 0 ) {

            // init empty files which we can still join against.

            new ExtractWriter( config, "/pr/out/rank_vector" ).close();
            new ExtractWriter( config, "/pr/out/teleportation_grant" ).close();

        }

        controller.merge( IterJob.Map.class,
                          new Input( "/pr/graph_by_source" ,
                                     "/pr/out/rank_vector" ,
                                     "/pr/out/dangling" ,
                                     "/pr/out/nonlinked" ,
                                     "broadcast:/pr/out/nr_nodes" ) ,
                          new Output( "shuffle:default",
                                      "broadcast:dangling_rank_sum" ) );

        controller.reduce( IterJob.Reduce.class,
                           new Input( "shuffle:default",
                                      "broadcast:/pr/out/nr_nodes",
                                      "broadcast:/pr/out/nr_dangling",
                                      "broadcast:/pr/out/teleport_grant" ),
                           new Output( "/pr/out/rank_vector",
                                       "broadcast:rank_sum" ) );

        // now reduce the broadcast rank sum to an individual file for analysis
        // and reading
        controller.reduce( GlobalRankSumJob.Reduce.class,
                           new Input( "shuffle:rank_sum" ),
                           new Output( "/pr/out/rank_sum" ) );

        // ***
        // 
        // write out the new ranking vector
        if ( step < iterations - 1 ) {

            // now compute the dangling rank sum for the next iteration

            controller.reduce( TeleportationGrantJob.Reduce.class, 
                               new Input( "shuffle:dangling_rank_sum",
                                          "broadcast:/pr/out/nr_nodes" ),
                               new Output( "/pr/out/teleportation_grant" ) );

        }

        ++step;
        
    }

    public void term() throws Exception {

        log.info( "Running term() stage." );

        // merge the rank vector, node metadata (indegree, outdegree) as well as name of the node, title, and description.
        controller.merge( MergeNodeAndRankMetaJob.Merge.class,
                          new Input( "/pr/out/node_metadata", "/pr/out/rank_vector", nodes_by_hashcode ),
                          new Output( "/pr/out/rank_metadata" ) );

        controller.sort( "/pr/out/rank_metadata",
                         "/pr/out/rank_metadata_by_rank",
                         RankMetadataByRankSortComparator.class );

        controller.sort( "/pr/out/rank_metadata",
                         "/pr/out/rank_metadata_by_indegree",
                         RankMetadataByIndegreeComparator.class );

    }
    
    /**
     * Run a full pagerank computation including init, iter, and shutdown.
     */
    public void exec() throws Exception {
        exec( true );
    }

    /**
     * Run a full pagerank computation including init, iter, and shutdown.
     */
    public void exec( boolean autoShutdown ) throws Exception {

        try {

            init();
            
            // init the empty rank_vector table ... we need to merge against it.

            // ***** ITER stage... 

/*
            for( int step = 0; step < iterations; ++step ) {
                iter();
            }

            term();

*/
            
            log.info( "Pagerank complete" );

        } finally {

            if ( autoShutdown )
                shutdown();
            
        }
            
    }

    /**
     * Shutdown the controller and release all resources.  Note that this must
     * be done in a finally block so that we don't leave the cluster in an
     * inconsistent state.
     */
    public void shutdown() {
        controller.shutdown();
    }

    public int getIterations() { 
        return this.iterations;
    }

    public void setIterations( int iterations ) { 
        this.iterations = iterations;
    }

}
