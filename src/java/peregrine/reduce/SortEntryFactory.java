
package peregrine.reduce;

import peregrine.*;
import peregrine.values.*;

public interface SortEntryFactory  {
    
    public SortEntry newSortEntry( StructReader key, StructReader value );

}
