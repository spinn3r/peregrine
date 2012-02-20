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

import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.util.*;
import peregrine.util.primitive.*;

public class GraphBuilder {

    private ExtractWriter writer;
    
    public GraphBuilder( ExtractWriter writer ) {
        this.writer = writer;
    }

    public void buildRandomGraph( int nr_nodes,
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
                addRecord( source, targets );
            
            // now output our progress... 
            int perc = (int)((i / (float)nr_nodes) * 100);

            if ( perc != last ) {
                System.out.printf( "%s%% ", perc );
            }

            last = perc;

        }

        System.out.printf( " done (Wrote %,d edges over %,d nodes)\n", edges, nr_nodes );

    }

    private long hash( String data ) {
        return LongBytes.toLong( Base64.decode( data ) );
    }
    
    public void addRecord( long source,
                           long... targets ) throws Exception {
        
        addRecord( source, Longs.toList( targets ) );
        
    }

    public void addRecord( String source,
                           String... targets ) throws Exception {

        List<Long> list = new ArrayList();

        for( String target : targets ) {
            list.add( hash( target ) );
        }

        addRecord( hash( source ) , list );
        
    }

    public void addRecord( long source,
                           List<Long> targets ) throws Exception {

        writer.write( StructReaders.hashcode( source ),
                      StructReaders.hashcode( targets ) );

    }
    
}
