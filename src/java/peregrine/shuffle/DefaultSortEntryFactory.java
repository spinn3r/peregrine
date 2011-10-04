
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
    
    public SortEntry newSortEntry( byte[] key, byte[] value ) {

        SortEntry entry = new SortEntry( key );

        Struct struct = new Struct();
        struct.fromBytes( value );

        entry.addValues( struct.read() );
        
        return entry;

    }

}
