package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.pagerank.*;

public class TestPagerank extends junit.framework.TestCase {

    public void test1() throws Exception {

        // TRY with three partitions... 
        Config.addPartitionMembership( 0, "cpu0" );
        Config.addPartitionMembership( 1, "cpu1" );
        
        String path = "/pr/test.graph";
        
        ExtractWriter writer = new ExtractWriter( path );

        buildGraph1( writer );
        
        writer.close();

        // I think a more ideal API would be Controller.exec( path, mapper, reducer );

        //FIXME: /pr/test.graph will NOT be sorted on input even though the
        //values are unique.... on stage two we won't be able to join against
        //it.
        
        Controller.map( NodeIndegreeJob.Map.class, path );
        Controller.reduce( NodeIndegreeJob.Reduce.class, null, new Output( "/pr/tmp/node_indegree" ) );

        Controller.map( Mapper.class, "/pr/test.graph" );
        Controller.reduce( Reducer.class, null, new Output( "/pr/test.graph_by_source" ) );

        //now create node metadata...
        Controller.mergeMapWithFullOuterJoin( NodeMetadataJob.Map.class,
                                              new Input( "/pr/tmp/node_indegree", "/pr/test.graph_by_source" ),
                                              new Output( new FileOutputReference( "/pr/out/node_metadata" ),
                                                          new FileOutputReference( "/pr/out/dangling" ),
                                                          new FileOutputReference( "/pr/out/nonlinked" ),
                                                          new BroadcastOutputReference( "nr_nodes" ) ) );

        String nr_nodes_path = "/pr/out/nr_nodes";

        Controller.reduce( NodeMetadataJob.Reduce.class,
                           new Input( new ShuffleInputReference( "nr_nodes" ) ),
                           new Output( nr_nodes_path ) );

        //now read in the output and make sure our results are correct...

        TestBroadcastMapReduce.assertValueOnAllPartitions( nr_nodes_path, 12 );

        // init the empty rank_vector table ... we need to merge against it.
        Controller.map( Mapper.class, new Input(), new Output( "/pr/out/rank_vector" ) );

        // FIXME: add nonlinked... 
        
        Controller.mergeMapWithFullOuterJoin( IterJob.Map.class,
                                              new Input( new FileInputReference( "/pr/test.graph_by_source" ),
                                                         new FileInputReference( "/pr/out/rank_vector" ),
                                                         new FileInputReference( "/pr/out/dangling" ),
                                                         new FileInputReference( "/pr/out/nonlinked" ),
                                                         new BroadcastInputReference( "/pr/out/nr_nodes" ) ),
                                              new Output( new BroadcastOutputReference( "dangling_rank_sum" ) ) );

        Controller.reduce( IterJob.Reduce.class,
                           null,
                           new Output( "/pr/out/rank_vector_new" ) );

        // now compute the dangling rank sum.. 

        Controller.reduce( TeleportationGrantJob.Reduce.class, 
                           new Input( new ShuffleInputReference( "dangling_rank_sum" ),
                                      new BroadcastInputReference( "/pr/out/nr_nodes" ) ),
                           new Output( "/pr/out/teleportation_rant" ) );
        
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
        new TestPagerank().test1();
    }

}