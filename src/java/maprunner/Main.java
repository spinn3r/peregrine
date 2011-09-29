package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.io.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.pagerank.*;

public class Main {

    public static void main( String[] args ) throws Exception {

        // TRY with three partitions... 
        Config.addPartitionMembership( 0, "cpu0" );
        Config.addPartitionMembership( 1, "cpu1" );

        String path = "/pr/test.graph";
        
        ExtractWriter writer = new ExtractWriter( path );

        buildRandomGraph( writer, 50, 10 );
        
        writer.close();

        // I think a more ideal API would be Controller.exec( path, mapper, reducer );

        Controller.map( NodeIndegreeJob.Map.class, path );
        Controller.reduce( NodeIndegreeJob.Reduce.class, "/pr/tmp/node_indegree" );

        //now create node metadata...

        Controller.mapWithFullOuterJoin( NodeMetadataJob.Map.class, "/pr/tmp/node_indegree", "/pr/test.graph" );

        Controller.reduce( NodeMetadataJob.Reduce.class, "/pr/out/node_metadata" );

    }

    public static void buildRandomGraph( ExtractWriter writer,
                                         int nr_nodes,
                                         int max_edges_per_node ) throws Exception {
        
        System.out.printf( "Creating nodes/links: %s\n", nr_nodes );

        int last = -1;

        Random r = new Random();

        int edges = 0;
        
        for( int i = 0; i < nr_nodes; ++i ) {

            int ref = max_edges_per_node;

            int gap = (int)Math.ceil( i / (float)ref ) ;

            int source = i;

            int first = (int)(gap * r.nextFloat());

            int target = first;

            List<Integer> targets = new ArrayList( max_edges_per_node );
            
            for( int j = 0; j < max_edges_per_node && j < i ; ++j ) {
                targets.add( target );
                target = target + gap;
            }

            edges += targets.size();

            if ( targets.size() > 0 )
                addRecord( writer, source, targets );
            
            // now output our progress... 
            int perc = (int)((i / (float)nr_nodes) * 100);

            if ( perc != last ) {
                System.out.printf( "%s%% ", perc );
            }

            last = perc;

        }

        System.out.printf( " done (Wrote %,d edges over %,d nodes)\n", edges, nr_nodes );

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
    
}