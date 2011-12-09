
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class DiskStat extends BaseStat implements Diffable<DiskStat> {

    BigDecimal reads             = new BigDecimal( 0 );
    BigDecimal writes            = new BigDecimal( 0 );

    BigDecimal readBytes         = new BigDecimal( 0 );
    BigDecimal writtenBytes      = new BigDecimal( 0 );
    BigDecimal timeSpentReading  = new BigDecimal( 0 );
    BigDecimal timeSpentWriting  = new BigDecimal( 0 );

    /**
     * Disk utilization over the interval [0.0 , 100.0].  This is -1 if not yet
     * computed.
     *
     * http://www.xaprb.com/blog/2010/01/09/how-linux-iostat-computes-its-results/
     * 
     * "%util is the total time spent doing I/Os, divided by the sampling
     * interval. This tells you how much of the time the device was busy, but
     * it doesn’t really tell you whether it’s reaching its limit of
     * throughput, because the device could be backed by many disks and hence
     * capable of multiple I/O operations simultaneously."
     */
    double util = 0.0;

    /**
     * avgrq-sz is the number of sectors divided by the number of I/O
     * operations.
     */
    double avg_req_size = 0.0;

    @Override
    public DiskStat diff( DiskStat after ) {
        
        DiskStat result = new DiskStat();
        
        result.name = name;
        result.duration = after.timestamp - timestamp;

        result.readBytes        = after.readBytes.subtract( readBytes );
        result.writtenBytes     = after.writtenBytes.subtract( writtenBytes );
        result.timeSpentReading = after.timeSpentReading.subtract( timeSpentReading );
        result.timeSpentWriting = after.timeSpentWriting.subtract( timeSpentWriting );
        result.reads            = after.reads.subtract( reads );
        result.writes           = after.writes.subtract( writes );

        result.init();
        
        return result;
        
    }

    /**
     * Compute the rate of this state over the given interval.
     */
    @Override
    public DiskStat rate( long interval ) {

        DiskStat result = new DiskStat();

        result.name = name;
        result.duration = interval;

        result.readBytes           = overInterval( readBytes, interval );
        result.writtenBytes        = overInterval( writtenBytes, interval );
        result.timeSpentReading    = overInterval( timeSpentReading, interval );
        result.timeSpentWriting    = overInterval( timeSpentWriting, interval );
        result.reads               = overInterval( reads, interval );
        result.writes              = overInterval( writes, interval );

        result.init();

        return result;
        
    }

    @Override
    public void init() {

        // right now these derived stats are not specified for absolute
        // measurements but only from diff or rate.

        if ( duration == timestamp )
            return;

        BigDecimal iotime = timeSpentReading.add( timeSpentWriting );

        util = iotime.divide( new BigDecimal( duration ), 2, RoundingMode.CEILING ).doubleValue();

        BigDecimal nr_ops = reads.add( writes );
        BigDecimal nr_bytes = readBytes.add( writtenBytes );

        // TODO: we can break average request size into reads and writes.

        if( ! isZero( nr_ops ) ) {
            avg_req_size = nr_bytes.divide( nr_ops, 2, RoundingMode.CEILING ).doubleValue();
        }

    }

    @Override
    public String toString() {

        StringBuilder buff = new StringBuilder();

        buff.append( String.format( "%10s %,15d %,15d %,15f %15f",
                                    name, readBytes.longValue(), writtenBytes.longValue(), util, avg_req_size ) );

        return buff.toString();
        
    }
    
}

