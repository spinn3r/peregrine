package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;

public class Membership {
    
    private int nr_hosts;

    protected Map<Partition,List<Host>> forward = new HashMap();

    protected Map<Host,List<Partition>> reverse = new HashMap();

    protected Map<Host,List<Partition>> primary = new HashMap();

    public Membership() {}
    
    /**
     * Create new membership from an explicit mapping.
     */
    public Membership( Map<Partition,List<Host>> forward,
                       Map<Host,List<Partition>> reverse,
                       Map<Host,List<Partition>> primary ) {

        this.forward = forward;
        this.reverse = reverse;
        this.primary = primary;
        
    }
    
    public Set<Partition> getPartitions() {
        return forward.keySet();
    }

    public List<Host> getHosts( Partition part ) {
        return forward.get( part );
    }

    public List<Partition> getPartitions( Host host ) {
        return reverse.get( host );
    }

    public List<Partition> getPrimary( Host host ) {
        return primary.get( host );
    }

    public void setPartition( Partition part, List<Host> hosts ) {
        forward.put( part, hosts );
        updateReverseMapping( part, hosts );
    }

    public int size() {
        return forward.size();
    }

    public String toMatrix() {
        
        StringBuffer buff = new StringBuffer();

        List<Host> hosts = new ArrayList();
        hosts.addAll( reverse.keySet() );

        Collections.sort( hosts );
        
        for( Host host : hosts ) {

            buff.append( String.format( "%35s: ", host.getName() ) );

            List<Partition> partitions = reverse.get( host );

            for( Partition part : partitions ) {

                buff.append( String.format( "%10s", part.getId() ) );

            }
            
            buff.append( "\n" );
            
        }

        String result = buff.toString();
        return result.substring( 0, result.length() - 1);

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
