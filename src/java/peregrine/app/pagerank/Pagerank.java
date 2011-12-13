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

            // TODO: I think I can elide this and the next step by reading the
            // input once and writing two two destinations but right now we
            // don't have split support.
            
            controller.map( NodeIndegreeJob.Map.class, path );

            controller.reduce( NodeIndegreeJob.Reduce.class,
                               new Input( "shuffle:default" ),
                               new Output( "/pr/tmp/node_indegree" ) );

            // sort the graph by source.. 
            controller.map( Mapper.class, path );
            
            controller.reduce( Reducer.class,
                               new Input( "shuffle:default" ),
                               new Output( "/pr/test.graph_by_source" ) );
            
            //now create node metadata...
            controller.merge( NodeMetadataJob.Map.class,
                              new Input( "/pr/tmp/node_indegree", "/pr/test.graph_by_source" ),
                              new Output( new FileOutputReference( "/pr/out/node_metadata" ),
                                          new FileOutputReference( "/pr/out/dangling" ),
                                          new FileOutputReference( "/pr/out/nonlinked" ),
                                          new BroadcastOutputReference( "nr_nodes" ),
                                          new BroadcastOutputReference( "nr_dangling" ) ) );

            controller.reduce( NodeMetadataJob.Reduce.class,
                               new Input( new ShuffleInputReference( "nr_nodes" ) ),
                               new Output( "/pr/out/nr_nodes" ) );

            controller.reduce( NodeMetadataJob.Reduce.class,
                               new Input( new ShuffleInputReference( "nr_dangling" ) ),
                               new Output( "/pr/out/nr_dangling" ) );

            // init the empty rank_vector table ... we need to merge against it.
            controller.map( Mapper.class, new Input(), new Output( "/pr/out/rank_vector" ) );
            
            controller.merge( IterJob.Map.class,
                              new Input( new FileInputReference( "/pr/test.graph_by_source" ),
                                         new FileInputReference( "/pr/out/rank_vector" ),
                                         new FileInputReference( "/pr/out/dangling" ),
                                         new FileInputReference( "/pr/out/nonlinked" ),
                                         new BroadcastInputReference( "/pr/out/nr_nodes" ) ),
                              new Output( new ShuffleOutputReference(),
                                          new BroadcastOutputReference( "dangling_rank_sum" ) ) );

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