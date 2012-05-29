package peregrine.app.pagerank;

import java.util.*;
import peregrine.*;
import peregrine.util.*;

/**
 * Join the wikipedia pages which have an ID to their nodes, which have an ID.
 * This is a merge job because we need two imputs.
 */
public class JoinPagesAndLinksJob {

    public static class Merge extends Merger {

        @Override
        public void merge( StructReader key,
                           List<StructReader> values ) {

            StructReader node = values.get( 0 );

            if ( node != null ) { 
                
                String page = node.readString();
                
                System.out.printf( "FIXME: %s\n", page );

            }

            System.out.printf( "FIXME: hello world\n" );
            
        }

    }

}