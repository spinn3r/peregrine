
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class InterfaceStat extends BaseStat implements Diffable<InterfaceStat> {
    
    BigDecimal readBytes     = new BigDecimal( 0 );
    BigDecimal writtenBytes  = new BigDecimal( 0 );

    @Override
    public InterfaceStat diff( InterfaceStat after ) {

        InterfaceStat result = new InterfaceStat();
        
        result.name = name;

        result.readBytes    = after.readBytes.subtract( readBytes );
        result.writtenBytes = after.writtenBytes.subtract( writtenBytes );

        result.duration = after.duration = duration;

        return result;
        
    }

    /**
     * Compute the rate of this state over the given interval.
     */
    @Override
    public InterfaceStat rate( long interval ) {

        InterfaceStat result = new InterfaceStat();

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

