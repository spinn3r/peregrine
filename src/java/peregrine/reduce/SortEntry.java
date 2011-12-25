
package peregrine.reduce;

import java.util.*;

import peregrine.*;

public final class SortEntry {

    public StructReader key;
    public byte[] keyAsByteArray;
    
    private List<StructReader> values = new ArrayList();

    public SortEntry( StructReader key ) {
        this.key = key;
        this.keyAsByteArray = key.toByteArray();
    }

    public void addValue( StructReader value ) {
        this.values.add( value );
    }

    public void addValues( List<StructReader> _values ) {
        this.values.addAll( _values );
    }
    
    public List<StructReader> getValues() {
        return this.values;
    }
    
}

