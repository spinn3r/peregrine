
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
    
    BigDecimal readBytes     = new BigDecimal( 0 );
    BigDecimal writtenBytes  = new BigDecimal( 0 );
    
    @Override
    public DiskStat diff( DiskStat after ) {
        
        DiskStat result = new DiskStat();
        
        result.name = name;
        result.duration = after.duration = duration;

        result.readBytes    = after.readBytes.subtract( readBytes );
        result.writtenBytes = after.writtenBytes.subtract( writtenBytes );

        return result;
        
    }

    /**
     * Compute the rate of this state over the given interval.
     */
    @Override
    public DiskStat rate( long interval ) {

        DiskStat result = new DiskStat();

        result.name = name;
        result.duration = duration;

        result.readBytes = overInterval( readBytes, interval );
        result.writtenBytes = overInterval( writtenBytes, interval );

        return result;
        
    }
    
    @Override
    public String toString() {

        StringBuilder buff = new StringBuilder();

        buff.append( String.format( "%10s %,20d %,20d",
                                    name, readBytes.longValue(), writtenBytes.longValue() ) );

        return buff.toString();
        
    }
    
}

