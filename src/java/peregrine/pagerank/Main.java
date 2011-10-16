package peregrine.pagerank;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;

import org.apache.log4j.xml.DOMConfigurator;

public class Main {

    private Config config;
    
    public Main( Config config ) {
        this.config = config;
    }
    
    public void exec() throws Exception {

        String path = "/pr/test.graph";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        buildGraph1( writer );
        
        writer.close();

        // I think a more ideal API would be Controller.exec( path, mapper, reducer );

        //FIXME: /pr/test.graph will NOT be sorted on input even though the
        //values are unique.... on stage two we won't be able to join against
        //it.

        Controller controller = new Controller( config );
        config.setPort( 11111 );
        
        // FIXME: I think I can elide this and the next step by reading the
        // input once and writing two two destinations.
        
        controller.map( NodeIndegreeJob.Map.class,
                        path );

        controller.reduce( NodeIndegreeJob.Reduce.class,
                           new Input(),
                           new Output( "/pr/tmp/node_indegree" ) );

        // sort the graph by source.. 
        controller.map( Mapper.class, path );
        controller.reduce( Reducer.class, new Input(), new Output( "/pr/test.graph_by_source" ) );

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

        //now read in the output and make sure our results are correct...

        //FIXME: this broke no the local version and we ned to add it back in.
        //TestBroadcastMapReduce.assertValueOnAllPartitions( "/pr/out/nr_nodes", 12 );

        // init the empty rank_vector table ... we need to merge against it.
        controller.map( Mapper.class, new Input(), new Output( "/pr/out/rank_vector" ) );

        // FIXME: add nonlinked... 
        
        controller.merge( IterJob.Map.class,
                          new Input( new FileInputReference( "/pr/test.graph_by_source" ),
                                     new FileInputReference( "/pr/out/rank_vector" ),
                                     new FileInputReference( "/pr/out/dangling" ),
                                     new FileInputReference( "/pr/out/nonlinked" ),
                                     new BroadcastInputReference( "/pr/out/nr_nodes" ) ),
                          new Output( new BroadcastOutputReference( "dangling_rank_sum" ) ) );

        controller.reduce( IterJob.Reduce.class,
                           new Input( new ShuffleInputReference(),
                                      new BroadcastInputReference( "/pr/out/nr_nodes" ) ),
                           new Output( "/pr/out/rank_vector_new" ) );

        // // now compute the dangling rank sum.. 

        // controller.reduce( TeleportationGrantJob.Reduce.class, 
        //                    new Input( new ShuffleInputReference( "dangling_rank_sum" ),
        //                               new BroadcastInputReference( "/pr/out/nr_nodes" ) ),
        //                    new Output( "/pr/out/teleportation_rant" ) );
        
    }

    public static void buildGraph1( ExtractWriter writer ) throws Exception { 

        // only 0 and 1 should be dangling.
        
        addRecord( writer, 2, 0, 1 );
        addRecord( writer, 3, 1, 2 );
        addRecord( writer, 4, 2, 3 );
        addRecord( writer, 5, 2, 3 );
        addRecord( writer, 6, 2, 3 );
        addRecord( writer, 7, 2, 3 );
        addRecord( writer, 8, 2, 3 );
        addRecord( writer, 9, 2, 3 );
        addRecord( writer, 10, 2, 3 );
        addRecord( writer, 11, 2, 3 );

    }

    public static void addRecord( ExtractWriter writer,
                                  int source,
                                  int... targets ) throws Exception {

        List<Integer> list = new ArrayList();

        for( int t : targets ) {
            list.add( t ) ;
        }

        addRecord( writer, source, list );
        
    }

    public static void addRecord( ExtractWriter writer,
                                  int source,
                                  List<Integer> targets ) throws Exception {

        boolean keyIsHashcode = true;

        byte[] hash = Hashcode.getHashcode( ""+source );
            
        ByteArrayKey key = new ByteArrayKey( hash );

        HashSetValue value = new HashSetValue();
        for ( int target : targets ) {
            byte[] data = Hashcode.getHashcode( Integer.toString( target ) );
            value.add( data );
        }
        
        writer.write( key, value, keyIsHashcode );

    }

    public static void main( String[] args ) throws Exception {

        //FIXME: make sure this machine is either the controller box or in the
        //list of hosts configured.  Otherwise it's pointless to run.
        
        DOMConfigurator.configure( "conf/log4j.xml" );
        Config config = Config.parse( "conf/peregrine.conf", "conf/peregrine.hosts" );

        new Main( config ).exec();
        
    }

}