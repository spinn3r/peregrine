
package peregrine.reduce;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;

public class FullTupleComparator implements Comparator<Tuple> {

    FullKeyComparator delegate = new FullKeyComparator();
    
    public int compare( Tuple t0, Tuple t1 ) {
        return delegate.compare( t0.key, t1.key );
    }
    
}

