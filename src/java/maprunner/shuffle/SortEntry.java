
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

    public SortRecord record;

    public List<byte[]> values = new ArrayList();
    
    public SortEntry( SortRecord record ) {
        this.record = record;
    }

    public int compareTo( Object obj ) {
        return record.compareTo( obj );
    }
    
}

