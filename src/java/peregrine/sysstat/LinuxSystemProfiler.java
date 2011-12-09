
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 * 
 * http://www.xaprb.com/blog/2010/01/09/how-linux-iostat-computes-its-results/
 * 
 * iostat is one of the most important tools for measuring disk performance, which
 * of course is very relevant for database administrators, whether your chosen
 * database is Postgres, MySQL, Oracle, or anything else that runs on
 * GNU/Linux. Have you ever wondered where statistics like await (average wait for
 * the request to complete) come from? If you look at the disk statistics the Linux
 * kernel makes available through files such as /proc/diskstats, you won’t see
 * await there. How does iostat compute await? For that matter, how does it compute
 * the average queue size, service time, and utilization? This blog post will show
 * you how that’s computed.
 * 
 * First, let’s look at the fields in /proc/diskstats. The order and location
 * varies between kernels, but the following applies to 2.6 kernels. For reads and
 * writes, the file contains the number of operations, number of operations merged
 * because they were adjacent, number of sectors, and number of milliseconds
 * spent. Those are available separately for reads and writes, although iostat
 * groups them together in some cases. Additionally, you can find the number of
 * operations in progress, total number of milliseconds during which I/Os were in
 * progress, and the weighted number of milliseconds spent doing I/Os. Those are
 * not available separately for reads and writes.
 * 
 * The last one is very important. The field showing the number of operations in
 * progress is transient — it shows you the instantaneous value, and this
 * “memoryless” property means you can’t use it to infer the number of I/O
 * operations that are in progress on average. But the last field has memory,
 * because it is defined as follows:
 * 
 * Field 11 — weighted # of milliseconds spent doing I/Os This field is incremented
 * at each I/O start, I/O completion, I/O merge, or read of these stats by the
 * number of I/Os in progress (field 9) times the number of milliseconds spent
 * doing I/O since the last update of this field. This can provide an easy measure
 * of both I/O completion time and the backlog that may be accumulating.
 * 
 * So the field indicates the total number of milliseconds that all requests have
 * been in progress. If two requests have been waiting 100ms, then 200ms is added
 * to the field. And thus it records what happened over the duration of the
 * sampling interval, not just what’s happening at the instant you look at the
 * file. We’ll come back to that later.
 * 
 * Now, given two samples of I/O statistics and the time elapsed between them, we
 * can easily compute everything iostat outputs in -dx mode. I’ll take them
 * slightly out of order to reflect how the computations are done internally.
 * 
 * rrqm/s is merely the incremental merges divided by the number of seconds elapsed.
 * 
 * wrqm/s is similarly simple, and r/s, w/s, rsec/s, and wsec/s are too.
 * 
 * avgrq-sz is the number of sectors divided by the number of I/O operations.
 * 
 * avgqu-sz is computed from the last field in the file — the one that has “memory”
 * — divided by the milliseconds elapsed. Hence the units cancel out and you just
 * get the average number of operations in progress during the time period. The
 * name (short for “average queue size”) is a little bit ambiguous. This value
 * doesn’t show how many operations were queued but not yet being serviced — it
 * shows how many were either in the queue waiting, or being serviced. The exact
 * wording of the kernel documentation is “…as requests are given to appropriate
 * struct request_queue and decremented as they finish.”
 * 
 * %util is the total time spent doing I/Os, divided by the sampling interval. This
 * tells you how much of the time the device was busy, but it doesn’t really tell
 * you whether it’s reaching its limit of throughput, because the device could be
 * backed by many disks and hence capable of multiple I/O operations
 * simultaneously.
 * 
 * await is the total time for all I/O operations summed, divided by the number of
 * I/O operations completed.
 * 
 * svctm is the most complex to derive. It is the utilization divided by the
 * throughput. You saw utilization above; the throughput is the number of I/O
 * operations in the time interval.
 * 
 * Although the computations and their results seem both simple and cryptic, it
 * turns out that you can derive a lot of information from the relationship between
 * these various numbers. This is one of those tools where a few lines of code have
 * a surprising amount of meaning, which is left for the reader to understand. I’ll
 * get more into that in the future.
 * 
 */
public class LinuxSystemProfiler extends BaseSystemProfiler {

    private static final Logger log = Logger.getLogger();

    @Override
    public StatMeta update() {

        try {
        
            last = current;
            
            current = capture();

            return current;
            
        } catch ( Throwable t ) {
            log.error( "Unable to handle update: " , t );
            return new StatMeta();
        }
        
    }

    private StatMeta capture() throws IOException {

        StatMeta statMeta = new StatMeta();

        captureDisk( statMeta );
        captureInterface( statMeta );
        captureProcessor( statMeta );
        
        return statMeta;
        
    }

    /**
     * Fields are documented here:
     * 
     * http://www.mjmwired.net/kernel/Documentation/iostats.txt
     *
     * An example line is here.
     *
     * 8 0 sda 1663183 50370 286417462 8075722 16114577 124388899 1123914366 612454228 0 23049653 620527121
     *
     * The primary ones we care about are:
     * 
     * 
     * Field  3 -- # of sectors read
     * Field  7 -- # of sectors written
     *
     * For reference ALL the fields are here:
     * 
     *	Field  1 -- # of reads completed
     *	    This is the total number of reads completed successfully.
     *	Field  2 -- # of reads merged, field 6 -- # of writes merged
     *	    Reads and writes which are adjacent to each other may be merged for
     *	    efficiency.  Thus two 4K reads may become one 8K read before it is
     *	    ultimately handed to the disk, and so it will be counted (and queued)
     *	    as only one I/O.  This field lets you know how often this was done.
     *	Field  3 -- # of sectors read
     *	    This is the total number of sectors read successfully.
     *	Field  4 -- # of milliseconds spent reading
     *	    This is the total number of milliseconds spent by all reads (as
     *	    measured from __make_request() to end_that_request_last()).
     *	Field  5 -- # of writes completed
     *	    This is the total number of writes completed successfully.
     *	Field  7 -- # of sectors written
     *	    This is the total number of sectors written successfully.
     *	Field  8 -- # of milliseconds spent writing
     *	    This is the total number of milliseconds spent by all writes (as
     *	    measured from __make_request() to end_that_request_last()).
     *	Field  9 -- # of I/Os currently in progress
     *	    The only field that should go to zero. Incremented as requests are
     *	    given to appropriate struct request_queue and decremented as they finish.
     *	Field 10 -- # of milliseconds spent doing I/Os
     *	    This field increases so long as field 9 is nonzero.
     *	Field 11 -- weighted # of milliseconds spent doing I/Os
     *	    This field is incremented at each I/O start, I/O completion, I/O
     *	    merge, or read of these stats by the number of I/Os in progress
     *	    (field 9) times the number of milliseconds spent doing I/O since the
     *	    last update of this field.  This can provide an easy measure of both
     *	    I/O completion time and the backlog that may be accumulating.
     * 
     */
    private void captureDisk( StatMeta statMeta ) throws IOException {

        String value = read( "/proc/diskstats" );
        
        String[] lines = value.split( "\n" );

        for( String line : lines ) {
            
            String[] fields = line.split( "[\t ]+" );

            if ( fields.length < 10 ) {
                continue;
            }

            // the name of the device.
            String field_disk                 = fields[3];

            String field_reads                = fields[4];
            String field_readsMerged          = fields[5];
            String field_sectorsRead          = fields[6];
            String field_timeSpentReading     = fields[7];
            String field_writes               = fields[8];
            String field_writesMerged         = fields[9];   
            String field_sectorsWritten       = fields[10];  // Field 7
            String field_timeSpentWriting     = fields[11];  // Field 8
            String field_pending              = fields[12];  // Field 9
            String field_time                 = fields[13];  // Field 10

            
            if ( getDisks() != null && ! getDisks().contains( field_disk ) ) {
                continue;
            }

            DiskStat stat = new DiskStat();
            stat.timestamp = statMeta.timestamp;
            stat.name = field_disk;

            stat.reads             = sectorReference( field_reads );
            stat.writes            = sectorReference( field_writes );

            stat.readBytes         = sectorReference( field_sectorsRead );
            stat.writtenBytes      = sectorReference( field_sectorsWritten );
            stat.timeSpentReading  = new BigDecimal( field_timeSpentReading );
            stat.timeSpentWriting  = new BigDecimal( field_timeSpentWriting );
            stat.time              = new BigDecimal( field_time );

            stat.readsMerged       = new BigDecimal( field_readsMerged );
            stat.writesMerged      = new BigDecimal( field_writesMerged );
            
            stat.init();
            
            statMeta.diskStats.add( stat );
            
        }

    }

    private void captureInterface( StatMeta statMeta ) throws IOException {

        String value = read( "/proc/net/dev" );

        String[] lines = value.split( "\n" );

        for( int i = 0; i < lines.length; ++i ) {

            if ( i <= 1 )
                continue;

            String line = lines[i];

            String[] fields = line.split( "[\t ]+" );

            String field_net = fields[1];
            field_net = field_net.substring( 0 , field_net.length() - 1 );

            if ( fields.length < 11 )
                continue;
            
            String field_receive_bytes  = fields[2];
            String field_transmit_bytes = fields[10];

            if ( getInterfaces() != null && ! getInterfaces().contains( field_net ) ) {
                continue;
            }

            InterfaceStat stat = new InterfaceStat();
            stat.timestamp = statMeta.timestamp;
            stat.name = field_net;

            stat.readBits = new BigDecimal( field_receive_bytes )
                .multiply( new BigDecimal( 8 ) )
                ;
            
            stat.writtenBits = new BigDecimal( field_transmit_bytes )
                .multiply( new BigDecimal( 8 ) )
                ;

            stat.init();
            
            statMeta.interfaceStats.add( stat );
            
        }

    }

    /**
     * http://www.linuxhowtos.org/System/procstat.htm
     * http://stackoverflow.com/questions/3017162/how-to-get-total-cpu-usage-in-linux-c
     */
    private void captureProcessor( StatMeta statMeta ) throws IOException {

        String value = read( "/proc/stat" );

        String[] lines = value.split( "\n" );

        for( String line : lines ) {
            
            String[] fields = line.split( "[\t ]+" );

            String field_cpu     = fields[0];

            if ( ! field_cpu.startsWith( "cpu" ) )
                continue;

            // user: normal processes executing in user mode
            // nice: niced processes executing in user mode
            // system: processes executing in kernel mode
            // idle: twiddling thumbs
            // iowait: waiting for I/O to complete
            // irq: servicing interrupts
            // softirq: servicing softirqs

            ProcessorStat stat = new ProcessorStat();
            stat.timestamp = statMeta.timestamp;

            stat.name     = field_cpu;
            stat.user     = new BigDecimal( fields[1] );
            stat.nice     = new BigDecimal( fields[2] );
            stat.system   = new BigDecimal( fields[3] );
            stat.idle     = new BigDecimal( fields[4] );
            stat.iowait   = new BigDecimal( fields[5] ); 
            stat.irq      = new BigDecimal( fields[6] );
            stat.softirq  = new BigDecimal( fields[7] );

            stat.init();

            statMeta.processorStats.add( stat );
            
        }
            
    }
        
    private void dump( String[] fields ) {
        for( int i = 0; i < fields.length; ++i ) {
            System.out.printf( "%s=%s\n", i , fields[i] );
        }
    }
    
    /**
     * Perform a consistent read on a /proc file.
     */
    private String read( String path ) throws IOException {

        File file = new File( path );
        
        RandomAccessFile fis = new RandomAccessFile( file, "r" );

        byte[] buff = new byte[16384];
        
        int count = fis.read( buff );

        byte[] result = new byte[ count ];
        System.arraycopy( buff, 0, result, 0, count );

        return new String( result );

    }

    public BigDecimal sectorReference( String value ) {
        
        return new BigDecimal( value ).multiply( new BigDecimal( 512 ) );    
        
    }
    
}

