
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
public class StatMeta extends BaseStat {

    public List<DiskStat>       diskStats        = new ArrayList();
    public List<InterfaceStat>  interfaceStats   = new ArrayList();
    public List<ProcessorStat>  processorStats   = new ArrayList();

    @Override
    public String toString() {

        StringBuilder buff = new StringBuilder();

        buff.append( String.format( "Stat duration %s (%,d ms).\n\n", format( duration ), duration ) );
        
        buff.append( String.format( "%10s %15s\n", "Processor", "%util" ) );
        buff.append( String.format( "%10s %15s\n", "---------", "-----" ) );

        for( ProcessorStat processor : processorStats ) {
            buff.append( processor + "\n" );
        }

        buff.append( "\n" );

        buff.append( format( "Disk", "tps", "reads", "writes", "bytes read", "bytes written", "avg read size", "avg write size", "avg req size", "%util", "\n" ) );
        buff.append( format( "----", "---", "-----", "------", "----------", "-------------", "-------------", "--------------", "-----------", "-----", "\n" ) );

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

    private String format( long duration ) {

        Calendar cal = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
        cal.setTimeInMillis( 61000L );
        
        String time = String.format( "%02d:%02d:%02d",
                                     cal.get( cal.HOUR_OF_DAY  ),
                                     cal.get( cal.MINUTE ),
                                     cal.get( cal.SECOND ) );

        return time;

    }
    
}

