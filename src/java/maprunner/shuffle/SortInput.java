
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

public final class SortInput {

    public SortEntry entry = null;

    private ChunkReader reader;
    
    public SortInput( ChunkReader reader ) throws IOException {
        this.reader = reader;
        this.next();
    }

    public void next() throws IOException {

        KeyValuePair pair = this.reader.readKeyValuePair();

        if ( pair != null ) {
            this.entry = new SortEntry( pair.key );
        } else {
            entry = null;
        }
        
    }
    
    public boolean isExhausted() {
        return entry == null;
    }

}
