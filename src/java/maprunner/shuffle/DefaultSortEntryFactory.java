
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

public class DefaultSortEntryFactory implements SortEntryFactory {
    
    public SortEntry newSortEntry( Tuple tuple ) {

        SortEntry entry = new SortEntry( tuple.key );

        Struct struct = new Struct();
        struct.fromBytes( tuple.value );

        entry.addValues( struct.read() );
        
        return entry;

    }

}
