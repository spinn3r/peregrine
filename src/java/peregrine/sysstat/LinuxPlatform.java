
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public class LinuxPlatform {

    private StatMeta current = null;

    private String disk;
    private String net;
    
    public LinuxPlatform( String disk, String net ) {
        this.disk = disk;
        this.net  = net;
    }
    
    public StatMeta update() throws IOException {

        StatMeta before = current;
        
        current = capture();

        StatMeta after = current;

        if ( before == null ) {
            return current;
        } else { 
            return diff( before, after );
        }
        
    }
    
    private StatMeta capture() throws IOException {

        StatMeta statMeta = new StatMeta();

        captureDisk( disk, statMeta );
        captureNetwork( net, statMeta );
        captureCPU( statMeta );
        
        return statMeta;
        
    }

    public String format( StatMeta meta ) {

        StringBuilder buff = new StringBuilder();

        for( DiskStat disk : meta.diskStats ) {

            buff.append( String.format( "%10s %,20d %,20d\n",
                                        disk.name, disk.readBytes.longValue(), disk.writtenBytes.longValue() ) );

        }

        for( NetworkStat net : meta.networkStats ) {

            buff.append( String.format( "%10s %,20d %,20d\n",
                                        net.name, net.readBytes.longValue(), net.writtenBytes.longValue() ) );

        }

        for( CPUStat cpu : meta.cpuStats ) {

            buff.append( String.format( "%10s %,20.2f\n",
                                        cpu.name, cpu.active ) );

        }
        
        return buff.toString();
        
    }

    private StatMeta diff( StatMeta before , StatMeta after ) {

        StatMeta diff = new StatMeta();

        // FIXME: these values can overflow and go back to zero.  We need to
        // detect this an adjust for it.

        diff( diff.diskStats, before.diskStats, after.diskStats );
        diff( diff.networkStats, before.networkStats, after.networkStats );
        diff( diff.cpuStats, before.cpuStats, after.cpuStats );
        
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
    
    /**
     * Fields are documented here:
     * 
     * http://www.mjmwired.net/kernel/Documentation/iostats.txt
     *
     * An example line is here.
     *
     * 8 0 sda 1663183 50370 286417462 8075722 16114577 124388899 1123914366 612454228 0 23049653 620527121
     * 
     * Field  3 -- # of sectors read
     * Field  7 -- # of sectors written
     * 
     */
    private void captureDisk( String disk, StatMeta statMeta ) throws IOException {

        String value = read( "/proc/diskstats" );
        
        String[] lines = value.split( "\n" );

        for( String line : lines ) {
            
            String[] fields = line.split( "[\t ]+" );

            if ( fields.length < 10 ) {
                continue;
            }

            String field_disk = fields[3];
            String field_sectorsRead = fields[6];
            String field_sectorsWritten = fields[10];

            if ( ! disk.equals( field_disk ) ) {
                continue;
            }

            DiskStat stat = new DiskStat();
            stat.name = field_disk;
            
            stat.readBytes    = sectorReference( field_sectorsRead );
            stat.writtenBytes = sectorReference( field_sectorsWritten );

            statMeta.diskStats.add( stat );
            
        }

    }

    private void captureNetwork( String net, StatMeta statMeta ) throws IOException {

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

            if ( ! field_net.equals( net ) )
                continue;

            NetworkStat stat = new NetworkStat();
            stat.name = field_net;
            
            stat.readBytes = new BigDecimal( field_receive_bytes );
            stat.writtenBytes = new BigDecimal( field_transmit_bytes );

            statMeta.networkStats.add( stat );
            
        }

    }

    /**
     * http://www.linuxhowtos.org/System/procstat.htm
     * http://stackoverflow.com/questions/3017162/how-to-get-total-cpu-usage-in-linux-c
     */
    private void captureCPU( StatMeta statMeta ) throws IOException {

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

            CPUStat stat = new CPUStat();

            stat.name     = field_cpu;
            stat.user     = new BigDecimal( fields[1] );
            stat.nice     = new BigDecimal( fields[2] );
            stat.system   = new BigDecimal( fields[3] );
            stat.idle     = new BigDecimal( fields[4] );
            stat.iowait   = new BigDecimal( fields[5] ); 
            stat.irq      = new BigDecimal( fields[6] );
            stat.softirq  = new BigDecimal( fields[7] );

            stat.init();

            statMeta.cpuStats.add( stat );
            
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

