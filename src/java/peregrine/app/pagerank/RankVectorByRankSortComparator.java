package peregrine.app.pagerank;

import java.util.*;
import java.io.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.reduce.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.sort.*;

import com.spinn3r.log5j.*;

/**
 * Map reduce job which computes the partition routing table.
 */
public class RankVectorByRankSortComparator extends StrictSortDescendingComparator {

    private DescendingSortComparator comparator = new DescendingSortComparator() {

            @Override
            public int compare( KeyValuePair pair0 , KeyValuePair pair1 ) {
                // make the result ascending... 
                return super.compare( pair0, pair1 ) * -1;
                
            }

        };

    /**
     * 
     * 
     *
     */
    public RankVectorByRankSortComparator() {
        super( 
    }

    @Override
    public StructReader getSortKey( StructReader key, StructReader value ) {
        return StructReaders.join( value.slice( 0, 8 ), key.slice() );
    }

}
