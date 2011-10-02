
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

public class DefaultSortEntryFactory implements SortEntryFactory {
    
    public SortEntry newSortEntry( Tuple tuple ) {

        SortEntry entry = new SortEntry( tuple.key );

        Struct struct = new Struct();
        struct.fromBytes( tuple.value );

        entry.addValues( struct.read() );
        
        return entry;

    }

}
