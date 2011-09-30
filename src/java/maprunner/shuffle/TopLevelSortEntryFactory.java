
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

public class TopLevelSortEntryFactory implements SortEntryFactory {
    
    public SortEntry newSortEntry( Tuple tuple ) {

        VarintWriter writer = new VarintWriter();

        // the first value is a literal... 
        SortEntry entry = new SortEntry( tuple.key );
        entry.addValue( tuple.value );
        
        return entry;

    }

}