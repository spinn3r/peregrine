
package peregrine.reduce;

import peregrine.util.*;

public class TopLevelSortEntryFactory implements SortEntryFactory {
    
    public SortEntry newSortEntry( byte[] key, byte[] value ) {

        VarintWriter writer = new VarintWriter();

        // the first value is a literal... 
        SortEntry entry = new SortEntry( key );
        entry.addValue( value );
        
        return entry;

    }

}