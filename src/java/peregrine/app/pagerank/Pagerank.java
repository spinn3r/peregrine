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
public class Pagerank extends Batch {
    
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

    /**
     */
    private int step = 0;

    /**
     */
    private boolean sortedGraph = false;

    private String graph_by_source = "/pr/graph_by_source";
    
    public Pagerank( Config config, String graph, String nodes_by_hashcode ) {
        this( config, graph, nodes_by_hashcode, false );
    }
    
    public Pagerank( Config config, String graph, String nodes_by_hashcode, boolean sortedGraph ) {

        this.config = config;
        this.graph = graph;
        this.nodes_by_hashcode = nodes_by_hashcode;

        setName( Pagerank.class.getName() );
        setDescription( getName() );

        prepare();
                    
    }

    /**
     * Init PR ... setup all our vectors, node metadata table, etc.
     */
    private void init() {

        // ***** INIT stage... 

        log.info( "Running init() stage." );
        
        // TODO: We can elide this and the next step by reading the input
        // once and writing two two destinations.  this would read from
        // 'graph' and then wrote to node_indegree and graph_by_source at the
        // same time.

        // ***
        //
        // compute the node_indegree 

        map( NodeIndegreeJob.Map.class,
             new Input( graph ),
             new Output( "shuffle:default" ) );
        
        reduce( new Job().setDelegate( NodeIndegreeJob.Reduce.class )
                         .setCombiner( NodeIndegreeJob.Combine.class )
                         .setInput( "shuffle:default" )
                         .setOutput( "/pr/tmp/node_indegree" ) );

        // ***
        //
        // sort the graph by source since we aren't certain to have have the
        // keys in the right order and store in graph_by_source for joining
        // across every iteration.  This is invariant so we should store it
        // to the filesystem.
        // 

        if ( sortedGraph == false ) {
        
            map( Mapper.class,
                 new Input( graph ),
                 new Output( "shuffle:default" ) );
            
            reduce( GraphBySourceJob.Reduce.class,
                    new Input( "shuffle:default" ),
                    new Output( graph_by_source ) );

        } else {
            graph_by_source = graph;
        }
            
        // ***
        //
        // now create node metadata...  This will write the dangling vector,
        // the nonlinked vector and node_metadata which are all invariant.

        merge( new Job().setDelegate( NodeMetadataJob.Map.class )
                        .setInput( "/pr/tmp/node_indegree", graph_by_source )
                        .setOutput( "/pr/out/node_metadata" ,
                                    "/pr/out/dangling" ,
                                    "/pr/out/nonlinked" ,
                                    "broadcast:nr_nodes" ,
                                    "broadcast:nr_dangling" ) );
        
        reduce( NodeMetadataJob.Reduce.class,
                new Input( "shuffle:nr_nodes" ),
                new Output( "/pr/out/nr_nodes" ) );

        reduce( NodeMetadataJob.Reduce.class,
                new Input( "shuffle:nr_dangling" ),
                new Output( "/pr/out/nr_dangling" ) );
        
        // init empty files which we can still join against.

        // make sure these files exist.
        truncate( "/pr/out/rank_vector" );
        truncate( "/pr/out/teleportation_grant" );

    }

    /**
     * Run one pagerank step.
     */
    private void iter() {

        merge( IterJob.Map.class,
               new Input( graph_by_source ,
                          "/pr/out/rank_vector" ,
                          "/pr/out/dangling" ,
                          "/pr/out/nonlinked" ,
                          "broadcast:/pr/out/nr_nodes" ) ,
               new Output( "shuffle:default",
                           "broadcast:dangling_rank_sum" ) );

        reduce( IterJob.Reduce.class,
                new Input( "shuffle:default",
                           "broadcast:/pr/out/nr_nodes",
                           "broadcast:/pr/out/nr_dangling",
                           "broadcast:/pr/out/teleport_grant" ),
                new Output( "/pr/out/rank_vector",
                            "broadcast:rank_sum" ) );
        
        // now reduce the broadcast rank sum to an individual file for analysis
        // and reading
        reduce( GlobalRankSumJob.Reduce.class,
                new Input( "shuffle:rank_sum" ),
                new Output( "/pr/out/rank_sum" ) );
        
        // ***
        // 
        // write out the new ranking vector
        if ( step < iterations - 1 ) {

            // now compute the dangling rank sum for the next iteration

            reduce( TeleportationGrantJob.Reduce.class, 
                    new Input( "shuffle:dangling_rank_sum",
                               "broadcast:/pr/out/nr_nodes" ),
                    new Output( "/pr/out/teleportation_grant" ) );
            
        }

        ++step;
        
    }

    private void term() {

        // merge the rank vector, node metadata (indegree, outdegree) as well as name of the node, title, and description.
        merge( MergeNodeAndRankMetaJob.Merge.class,
               new Input( "/pr/out/node_metadata", "/pr/out/rank_vector", nodes_by_hashcode ),
               new Output( "/pr/out/rank_metadata" ) );
        
        sort( "/pr/out/rank_metadata",
              "/pr/out/rank_metadata_by_rank",
              RankMetadataByRankSortComparator.class );
        
        sort( "/pr/out/rank_metadata",
              "/pr/out/rank_metadata_by_indegree",
              RankMetadataByIndegreeComparator.class );

    }

    private void prepare() {

        init();
        
        // init the empty rank_vector table ... we need to merge against it.

        // ***** ITER stage... 

        for( int step = 0; step < iterations; ++step ) {
            iter();
        }

        term();

    }
    
    public int getIterations() { 
        return this.iterations;
    }

    public void setIterations( int iterations ) { 
        this.iterations = iterations;
    }

}
