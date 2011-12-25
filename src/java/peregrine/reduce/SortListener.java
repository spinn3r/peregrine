package peregrine.reduce;

import java.util.*;
import peregrine.values.*;

public interface SortListener {

    public void onFinalValue( StructReader key, List<StructReader> values );
    
}