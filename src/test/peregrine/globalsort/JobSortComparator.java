package peregrine.globalsort;

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
public class JobSortComparator extends StrictSortComparator {

    @Override
    public StructReader getSortKey( StructReader key, StructReader value ) {
        // read the first 8 bytes which is the long representation
        // of what we should sort be sorting by.
        return StructReaders.join( value.slice( 0, 8 ), key.slice() );
    }

}
