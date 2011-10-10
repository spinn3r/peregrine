package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;

public class Membership {

    private Map<Partition,List<Host>> delegate = new HashMap();

    private Map<Host,List<Partition>> reverse = new HashMap();

    public Set<Partition> getPartitions() {
        return delegate.keySet();
    }

    public List<Host> getHosts( Partition part ) {
        return delegate.get( part );
    }

    public List<Partition> getPartitions( Host host ) {

        System.out.printf( "FIXME: %s\n", reverse );
        
        return reverse.get( host );
        
    }
    
    public void setPartition( Partition part, List<Host> hosts ) {
        delegate.put( part, hosts );
        updateReverseMapping( part, hosts );
    }

    public int size() {
        return delegate.size();
    }

    private void updateReverseMapping( Partition part, List<Host> hosts ) {

        for( Host host : hosts ) {

            List<Partition> partitions = reverse.get( host );

            if ( partitions == null ) {
                partitions = new ArrayList();
                reverse.put( host, partitions );
            }

            partitions.add( part );
            
        }
        
    }
    
}
        