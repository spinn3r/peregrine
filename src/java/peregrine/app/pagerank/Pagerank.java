package peregrine.app.pagerank;

import peregrine.*;
import peregrine.config.Config;
import peregrine.controller.*;
import peregrine.io.*;

import com.spinn3r.log5j.Logger;

public class Pagerank {

    private static final Logger log = Logger.getLogger();

    private Config config;
    
    public Pagerank( Config config ) {
        this.config = config;
    }
    
    public void exec( String path ) throws Exception {

        Controller controller = new Controller( config );

        try {

            // TODO: We can elide this and the next step by reading the input
            // once and writing two two destinations.  this would read from
            // 'path' and then wrote to node_indegree and graph_by_source at the
            // same time.
            
            controller.map( NodeIndegreeJob.Map.class, path );

            controller.reduce( NodeIndegreeJob.Reduce.class,
                               new Input( "shuffle:default" ),
                               new Output( "/pr/tmp/node_indegree" ) );

            // sort the graph by source since we aren't gauranteed to have have
            // the keys in the right order.
            controller.map( Mapper.class, path );
            
            controller.reduce( Reducer.class,
                               new Input( "shuffle:default" ),
                               new Output( "/pr/test.graph_by_source" ) );
            
            //now create node metadata...
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
            controller.map( Mapper.class,
                            new Input(),
                            new Output( "/pr/out/rank_vector" ) );
            
            controller.merge( IterJob.Map.class,
                              new Input( "/pr/test.graph_by_source" ,
                                         "/pr/out/rank_vector" ,
                                         "/pr/out/dangling" ,
                                         "/pr/out/nonlinked" ,
                                         "broadcast:/pr/out/nr_nodes" ) ,
                              new Output( "shuffle:default",
                                          "broadcast:dangling_rank_sum" ) );

            controller.reduce( IterJob.Reduce.class,
                               new Input( new ShuffleInputReference(),
                                          new BroadcastInputReference( "/pr/out/nr_nodes" ) ),
                               new Output( "/pr/out/rank_vector_new" ) );

            // now compute the dangling rank sum for the next iteration

            controller.reduce( TeleportationGrantJob.Reduce.class, 
                               new Input( new ShuffleInputReference( "dangling_rank_sum" ),
                                          new BroadcastInputReference( "/pr/out/nr_nodes" ) ),
                               new Output( "/pr/out/teleportation_grant" ) );

            log.info( "Pagerank complete" );
            
        } finally {

            // Shutdown the controller and release all resources.  Note that
            // this must be done in a finally block so that we don't leave the
            // cluster in an inconsistent state.
            
            controller.shutdown();
            
        }
            
    }

}