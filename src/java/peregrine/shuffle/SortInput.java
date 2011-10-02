
package peregrine.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;

public final class SortInput {

    public SortEntry entry = null;

    private ChunkReader reader;
    private SortEntryFactory sortEntryFactory;
    
    public SortInput( ChunkReader reader,
                      SortEntryFactory sortEntryFactory ) throws IOException {

        this.reader = reader;
        this.sortEntryFactory = sortEntryFactory;
        this.next();

    }

    public void next() throws IOException {

        Tuple tuple = this.reader.read();

        if ( tuple != null ) {
            this.entry = sortEntryFactory.newSortEntry( tuple );
        } else {
            entry = null;
        }
        
    }
    
    public boolean isExhausted() {
        return entry == null;
    }

}
