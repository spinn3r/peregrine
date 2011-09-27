
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.io.*;

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
