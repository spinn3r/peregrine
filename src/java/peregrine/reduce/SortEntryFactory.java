
package peregrine.reduce;

import peregrine.*;

public interface SortEntryFactory  {
    
    public SortEntry newSortEntry( StructReader key, StructReader value );

}
