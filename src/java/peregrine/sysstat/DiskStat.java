
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class DiskStat implements Diffable<DiskStat> {

    String name = null;
    
    BigDecimal readBytes     = new BigDecimal( 0 );
    BigDecimal writtenBytes  = new BigDecimal( 0 );

    @Override
    public DiskStat diff( DiskStat after ) {

        System.out.printf( "FIXME: going to diff %s vs %s\n", this, after );
        
        DiskStat result = new DiskStat();
        
        result.name = name;

        result.readBytes    = after.readBytes.subtract( readBytes );
        result.writtenBytes = after.writtenBytes.subtract( writtenBytes );

        return this;
        
    }

    @Override
    public String toString() {

        return String.format( "%10s %,20d %,20d",
                              name, readBytes.longValue(), writtenBytes.longValue() );

    }
    
}

