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

        return StructReaders.join( prefix, key.slice() );

    }

}
