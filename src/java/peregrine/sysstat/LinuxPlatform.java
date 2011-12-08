
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public class LinuxPlatform {

    private StatMeta current = null;

    private String dev;
    
    public LinuxPlatform( String dev ) {
        this.dev = dev;
    }
    
    public StatMeta update() throws IOException {

        StatMeta before = current;
        
        current = capture();

        StatMeta after = current;

        return diff( before, after );
        
    }
    
    private StatMeta capture() throws IOException {

        StatMeta statMeta = new StatMeta();

        captureDiskstats( dev, statMeta );

        return statMeta;
        
    }

    private String format( StatMeta meta ) throws IOException {

        return String.format( "%10s %,10d %,10d",
                              dev, meta.readBytes.longValue(), meta.writtenBytes.longValue() );
        
    }

    private StatMeta diff( StatMeta before , StatMeta after ) {

        StatMeta diff = new StatMeta();

        diff.readBytes    = after.readBytes.subtract( before.readBytes );
        diff.writtenBytes = after.writtenBytes.subtract( before.writtenBytes );

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
    private void captureDiskstats( String dev, StatMeta statMeta ) throws IOException {

        File file = new File( "/proc/diskstats" );
        
        RandomAccessFile fis = new RandomAccessFile( file, "r" );

        byte[] buff = new byte[16384];
        
        int count = fis.read( buff );

        byte[] result = new byte[ count ];
        System.arraycopy( buff, 0, result, 0, count );

        String value = new String( result );
        
        String[] lines = value.split( "\n" );

        for( String line : lines ) {

            System.out.printf( "FIXME: line: %s\n", line );
            
            String[] fields = line.split( "[\t ]+" );

            if ( fields.length < 10 ) {
                System.out.printf( "FIXME: skipping: %,d\n", fields.length );
                continue;
            }

            String field_dev = fields[2];
            String field_sectorsRead = fields[5];
            String field_sectorsWritten = fields[9];

            System.out.printf( "FIXME field_dev: %s of length %s\n", field_dev, fields.length );

            if ( ! dev.equals( field_dev ) ) {
                continue;
            }

            statMeta.readBytes    = new SectorReference( field_sectorsRead );
            statMeta.writtenBytes = new SectorReference( field_sectorsWritten );
            
        }

    }

    class SectorReference extends BigDecimal {

        public SectorReference( String value ) {
            super( value );
            // sectors are in units of 512 bytes.
            multiply( new BigDecimal( 512 ) );
        }
        
    }
    
}

