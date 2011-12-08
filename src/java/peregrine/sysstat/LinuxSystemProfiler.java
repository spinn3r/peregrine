
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public class LinuxSystemProfiler extends BaseSystemProfiler {

    private static final Logger log = Logger.getLogger();

    @Override
    public StatMeta update() {

        try {
        
            last = current;
            
            current = capture();

            return current;
            
        } catch ( IOException e ) {
            log.error( "Unable to handle update: " , e );
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
            String field_sectorsWritten       = fields[10];
            String field_timeSpentWriting     = fields[11];

            if ( getDisks() != null && ! getDisks().contains( field_disk ) ) {
                continue;
            }

            DiskStat stat = new DiskStat();
            stat.name = field_disk;

            stat.reads             = sectorReference( field_reads );
            stat.writes            = sectorReference( field_writes );

            stat.readBytes         = sectorReference( field_sectorsRead );
            stat.writtenBytes      = sectorReference( field_sectorsWritten );
            stat.timeSpentReading  = new BigDecimal( field_timeSpentReading );
            stat.timeSpentWriting  = new BigDecimal( field_timeSpentWriting );
            
            statMeta.diskStats.add( stat );
            
        }

    }

    private void captureInterface( StatMeta statMeta ) throws IOException {

        String value = read( "/proc/net/dev" );

        String[] lines = value.split( "\n" );

        for( String line : lines ) {
            
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
            stat.name = field_net;
            
            stat.readBytes = new BigDecimal( field_receive_bytes );
            stat.writtenBytes = new BigDecimal( field_transmit_bytes );

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

