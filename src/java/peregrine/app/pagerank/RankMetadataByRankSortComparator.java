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

        StructReader prefix = value.slice( offset, DoubleBytes.LENGTH );

        //TODO: make this a range betwen 0 and 2^64 as an experiment.  We may
        //consider just using the float representation if it works correctly.
        prefix = StructReaders.wrap( prefix.readDouble() * Long.MAX_VALUE );
        
        return StructReaders.join( prefix, key.slice() );

    }

}
