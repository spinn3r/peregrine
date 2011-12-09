
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
    BigDecimal time              = new BigDecimal( 0 );

    BigDecimal readsMerged       = new BigDecimal( 0 );
    BigDecimal writesMerged      = new BigDecimal( 0 );
    
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
    double avg_io_size   = 0.0;

    double avg_read_size  = 0.0;
    double avg_write_size = 0.0;

    BigDecimal tps = new BigDecimal( 0 );
    
    @Override
    public DiskStat diff( DiskStat after ) {
        
        DiskStat result = new DiskStat();
        
        result.name = name;
        result.duration = after.timestamp - timestamp;

        result.readBytes        = after.readBytes.subtract( readBytes );
        result.writtenBytes     = after.writtenBytes.subtract( writtenBytes );
        result.timeSpentReading = after.timeSpentReading.subtract( timeSpentReading );
        result.timeSpentWriting = after.timeSpentWriting.subtract( timeSpentWriting );
        result.time             = after.time.subtract( time );
        result.reads            = after.reads.subtract( reads );
        result.writes           = after.writes.subtract( writes );
        result.readsMerged      = after.readsMerged.subtract( readsMerged );
        result.writesMerged     = after.writesMerged.subtract( writesMerged );

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
        result.time                = overInterval( time, interval );
        result.reads               = overInterval( reads, interval );
        result.writes              = overInterval( writes, interval );

        result.readsMerged         = overInterval( readsMerged, interval );
        result.writesMerged        = overInterval( writesMerged, interval );

        result.init();

        return result;
        
    }

    @Override
    public void init() {

        // it is important to use time here because we can't use reading and
        // writing as this would be greater than 100% 
        BigDecimal iotime = time;

        util = iotime.divide( new BigDecimal( duration ), 2, RoundingMode.CEILING ).doubleValue() * 100;

        BigDecimal nr_ops = reads.add( writes );
        BigDecimal nr_bytes = readBytes.add( writtenBytes );

        // TODO: we can break average request size into reads and writes.

        if( ! isZero( nr_ops ) ) {
            avg_io_size = nr_bytes.divide( nr_ops, 2, RoundingMode.CEILING ).doubleValue();
        }

        if( ! isZero( reads ) ) {
            avg_read_size = nr_bytes.divide( reads, 2, RoundingMode.CEILING ).doubleValue();
        }

        if( ! isZero( writes ) ) {
            avg_write_size = nr_bytes.divide( writes, 2, RoundingMode.CEILING ).doubleValue();
        }

        tps = reads.add( writes );
        
    }

    @Override
    public String toString() {

        StringBuilder buff = new StringBuilder();

        buff.append( format( name,
                             tps.longValue(),
                             reads.intValue(),
                             writes.intValue(),
                             readBytes.longValue(),
                             writtenBytes.longValue(),
                             avg_read_size,
                             avg_write_size,
                             avg_io_size,
                             util ) );

        return buff.toString();
        
    }
    
}

