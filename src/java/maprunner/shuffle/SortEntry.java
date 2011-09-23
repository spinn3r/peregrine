
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

public final class SortEntry implements SortRecord {

    private SortRecord record;

    public List<byte[]> values = new ArrayList();

    private long keycmp;
    
    public SortEntry( SortRecord record ) {
        keycmp = ((Tuple)record).keycmp;
    }

    public long longValue() {
        return keycmp;
    }

    public long compareTo( SortRecord record ) {
        return longValue() - record.longValue();
    }
    
}

