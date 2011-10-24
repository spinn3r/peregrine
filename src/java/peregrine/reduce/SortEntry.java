
package peregrine.reduce;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;

public final class SortEntry {

    public byte[] key;

    private List<byte[]> values = new ArrayList();

    public SortEntry( byte[] key ) {
        this.key = key;
    }

    public void addValue( byte[] value ) {
        this.values.add( value );
    }

    public void addValues( List<byte[]> _values ) {
        this.values.addAll( _values );
    }
    
    public List<byte[]> getValues() {
        return this.values;
    }
    
}

