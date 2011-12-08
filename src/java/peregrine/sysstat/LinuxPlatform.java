
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
    
        return statMeta;
        
    }

    public String format( StatMeta meta ) {

        StringBuilder buff = new StringBuilder();

        buff.append( String.format( "%10s %,20d %,20d\n",
                                    disk, meta.disk.readBytes.longValue(), meta.disk.writtenBytes.longValue() ) );

        buff.append( String.format( "%10s %,20d %,20d\n",
                                    net, meta.network.readBytes.longValue(), meta.network.writtenBytes.longValue() ) );

        return buff.toString();
        
    }

    private StatMeta diff( StatMeta before , StatMeta after ) {

        StatMeta diff = new StatMeta();

        // FIXME: these values can overflow and go back to zero.  We need to
        // detect this an adjust for it.

        diff.disk.diff( before.disk, after.disk );
        diff.network.diff( before.network, after.network );

        return diff;
        
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

            statMeta.disk.readBytes    = sectorReference( field_sectorsRead );
            statMeta.disk.writtenBytes = sectorReference( field_sectorsWritten );
            
        }

    }

    private void captureNetwork( String net, StatMeta statMeta ) throws IOException {

        String value = read( "/proc/net/dev" );

        String[] lines = value.split( "\n" );

        for( String line : lines ) {
            
            String[] fields = line.split( "[\t ]+" );

            String field_net = fields[1];
            field_net = field_net.substring( 0 , field_net.length() - 1 );

            String field_receive_bytes  = fields[2];
            String field_transmit_bytes = fields[10];

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

