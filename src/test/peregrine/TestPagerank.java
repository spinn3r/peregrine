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
        Controller.reduce( Reducer.class, null, new Output( "/pr/test.graph.sorted" ) );

        //now create node metadata...
        Controller.mergeMapWithFullOuterJoin( NodeMetadataJob.Map.class,
                                              new Input( "/pr/tmp/node_indegree", "/pr/test.graph.sorted" ),
                                              new Output( "/pr/out/node_metadata", "/pr/out/dangling", "/pr/out/nonlinked" ) );

        //FIXME: hint about the fact that these keys are pre-sorted
        //Controller.reduce( NodeMetadataJob.Reduce.class, );

        //now read in the output and make sure our results are correct...
        
    }

    public static void buildGraph1( ExtractWriter writer ) throws Exception { 

        addRecord( writer, 2, 0, 1 );
        addRecord( writer, 3, 1, 2 );
        addRecord( writer, 4, 2, 3 );

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