
package peregrine.reduce;

import peregrine.*;
import peregrine.util.*;
import peregrine.values.*;

public class TopLevelSortEntryFactory implements SortEntryFactory {
    
    public SortEntry newSortEntry( StructReader key, StructReader value ) {

        // the first value is a literal... 
        SortEntry entry = new SortEntry( key );
        entry.addValue( value );
        
        return entry;

    }

}