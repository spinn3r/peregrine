package peregrine.io;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;

public final class Tuple {

    public byte[] key = null;
    public byte[] value = null;

    public long keycmp;
    
    public Tuple( byte[] key, byte[] value ) {

        this.key = key;
        this.value = value;
        
    }

    public Tuple( Key key, Value value ) {

        this.key = key.toBytes();
        this.value = value.toBytes();

    }

}
