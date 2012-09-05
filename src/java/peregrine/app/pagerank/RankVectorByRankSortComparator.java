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

    @Override
    public StructReader getSortKey( StructReader key, StructReader value ) {
        return StructReaders.join( value.slice( 0, 8 ), key.slice() );
    }

}
