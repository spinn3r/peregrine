package peregrine.app.pagerank;

import java.util.*;
import java.io.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.*;
import peregrine.reduce.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.sort.*;

import com.spinn3r.log5j.*;

/**
 * 
 */
public class RankMetadataByRankSortComparator extends StrictSortDescendingComparator {

    @Override
    public StructReader getSortKey( StructReader key, StructReader value ) {

        int offset = IntBytes.LENGTH + IntBytes.LENGTH;
        
        return StructReaders.join( value.slice( 0, DoubleBytes.LENGTH ), key.slice() );

    }

}
