
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.io.*;

public class FullTupleComparator implements Comparator<Tuple> {

    FullKeyComparator delegate = new FullKeyComparator();
    
    public int compare( Tuple t0, Tuple t1 ) {
        return delegate.compare( t0.key, t1.key );
    }
    
}

