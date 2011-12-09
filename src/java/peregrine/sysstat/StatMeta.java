
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 * Stats for disk throughput and interface throughput.
 */
public class StatMeta {

    public List<DiskStat>       diskStats        = new ArrayList();
    public List<InterfaceStat>  interfaceStats   = new ArrayList();
    public List<ProcessorStat>  processorStats   = new ArrayList();

    public long timestamp = System.currentTimeMillis();

    @Override
    public String toString() {

        StringBuilder buff = new StringBuilder();

        buff.append( String.format( "%10s %15s\n", "Processor", "%util" ) );
        buff.append( String.format( "%10s %15s\n", "---------", "-----" ) );

        for( ProcessorStat processor : processorStats ) {
            buff.append( processor + "\n" );
        }

        buff.append( "\n" );

        buff.append( String.format( "%10s %15s %15s %15s %15s %15s %15s\n", "Disk", "reads", "writes", "bytes read", "bytes written", "Avg req size", "%util" ) );
        buff.append( String.format( "%10s %15s %15s %15s %15s %15s %15s\n", "----", "-----", "------", "----------", "-------------", "------------", "-----" ) );

        for( DiskStat disk : diskStats ) {
            buff.append( disk + "\n" );
        }

        buff.append( "\n" );

        buff.append( String.format( "%10s %15s %15s\n", "Interface", "bits rx",  "bits tx" ) );
        buff.append( String.format( "%10s %15s %15s\n", "---------", "-------",  "-------" ) );

        for( InterfaceStat net : interfaceStats ) {
            buff.append( net + "\n" );
        }

        return buff.toString();
        
    }

}

