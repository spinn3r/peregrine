
package peregrine.reduce;

import peregrine.values.*;

public class DefaultSortEntryFactory implements SortEntryFactory {
    
    public SortEntry newSortEntry( byte[] key, byte[] value ) {

        SortEntry entry = new SortEntry( key );

        Struct struct = new Struct();
        struct.fromBytes( value );

        entry.addValues( struct.read() );
        
        return entry;

    }

}
