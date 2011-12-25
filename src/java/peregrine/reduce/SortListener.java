package peregrine.reduce;

import java.util.*;

import peregrine.*;

public interface SortListener {

    public void onFinalValue( StructReader key, List<StructReader> values );
    
}