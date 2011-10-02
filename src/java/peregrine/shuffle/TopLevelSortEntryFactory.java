
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

public class TopLevelSortEntryFactory implements SortEntryFactory {
    
    public SortEntry newSortEntry( Tuple tuple ) {

        VarintWriter writer = new VarintWriter();

        // the first value is a literal... 
        SortEntry entry = new SortEntry( tuple.key );
        entry.addValue( tuple.value );
        
        return entry;

    }

}