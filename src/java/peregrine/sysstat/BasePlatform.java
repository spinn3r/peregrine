
package peregrine.sysstat;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public abstract class BasePlatform implements Platform {

    protected StatMeta last = null;
    protected StatMeta current = null;

    private long interval = 1000L;
    
    private Set<String> disks = null;
    private Set<String> processors = null;
    private Set<String> interfaces = null;

    public Set<String> getInterfaces() { 
        return this.interfaces;
    }

    public void setInterfaces( Set<String> interfaces ) { 
        this.interfaces = interfaces;
    }

    public Set<String> getProcessors() { 
        return this.processors;
    }

    public void setProcessors( Set<String> processors ) { 
        this.processors = processors;
    }

    public Set<String> getDisks() { 
        return this.disks;
    }

    public void setDisks( Set<String> disks ) { 
        this.disks = disks;
    }

    public long getInterval() { 
        return this.interval;
    }

    public void setInterval( long interval ) { 
        this.interval = interval;
    }

    @Override
    public StatMeta diff() {

        if ( last == null || current == null )
            return null;
        
        return diff( last, current );

    }

    private StatMeta diff( StatMeta before , StatMeta after ) {

        StatMeta diff = new StatMeta();

        // FIXME: these values can overflow and go back to zero.  We need to
        // detect this an adjust for it.

        diff( diff.diskStats, before.diskStats, after.diskStats );
        diff( diff.interfaceStats, before.interfaceStats, after.interfaceStats );
        diff( diff.processorStats, before.processorStats, after.processorStats );
        
        return diff;
        
    }

    private void diff( List target ,
                       List before,
                       List after ) {

        // TODO: I don't understand why I can't use generics for this.
        
        for( int i = 0; i < before.size(); ++i ) {
            target.add( ((Diffable)before.get( i )).diff( after.get( i ) ) );
        }

    }

    @Override
    public StatMeta rate() {

        StatMeta diff = diff();

        if ( diff == null )
            return diff;

        StatMeta result = new StatMeta();

        for( DiskStat stat : diff.diskStats ) {
            result.diskStats.add( stat.rate( getInterval() ) );
        }

        for( InterfaceStat stat : diff.interfaceStats ) {
            result.interfaceStats.add( stat.rate( getInterval() ) );
        }

        for( ProcessorStat stat : diff.processorStats ) {
            result.processorStats.add( stat.rate( getInterval() ) );
        }

        return result;
        
    }
    
}