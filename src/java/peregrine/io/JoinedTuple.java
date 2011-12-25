package peregrine.io;

import java.util.*;

import peregrine.*;

public final class JoinedTuple {

    public StructReader key = null;
    public List<StructReader> values = null;
    
    public JoinedTuple( StructReader key, List<StructReader> values ) {

        this.key = key;
        this.values = values;
        
    }

}
