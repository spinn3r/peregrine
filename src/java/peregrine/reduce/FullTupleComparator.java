
package peregrine.reduce;

import java.util.*;
import peregrine.io.*;

public class FullTupleComparator implements Comparator<Tuple> {

    FullKeyComparator delegate = new FullKeyComparator();
    
    public int compare( Tuple t0, Tuple t1 ) {
        return delegate.compare( t0.key, t1.key );
    }
    
}

