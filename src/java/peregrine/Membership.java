package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;

public class Membership {

    private Map<Partition,List<Host>> delegate = new HashMap();

    public Set<Partition> getPartitions() {
        return delegate.keySet();
    }

    public List<Host> getHosts( Partition part ) {
        return delegate.get( part );
    }

    public void setPartition( Partition part, List<Host> hosts ) {
        delegate.put( part, hosts );
    }

    public int size() {
        return delegate.size();
    }
    
}
        