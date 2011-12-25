package peregrine.app.pagerank;

import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.util.primitive.*;

public class GraphBuilder {

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

            long first = (int)(gap * r.nextFloat());

            long target = first;

            List<Long> targets = new ArrayList( max_edges_per_node );
            
            for( long j = 0; j < max_edges_per_node && j < i ; ++j ) {
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
                                  long source,
                                  long... targets ) throws Exception {

        List<Long> list = new ArrayList();

        for( long t : targets ) {
            list.add( t ) ;
        }

        addRecord( writer, source, list );
        
    }
    
    public static void addRecord( ExtractWriter writer,
                                  long source,
                                  List<Long> targets ) throws Exception {

        StructReader key = StructReaders.hashcode( source );

        StructReader value = StructReaders.hashcode( targets );
        
        writer.write( key, value );

    }
    
}