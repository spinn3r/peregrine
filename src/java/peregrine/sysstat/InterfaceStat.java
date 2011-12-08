
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
    
    BigDecimal readBits     = new BigDecimal( 0 );
    BigDecimal writtenBits  = new BigDecimal( 0 );

    @Override
    public InterfaceStat diff( InterfaceStat after ) {

        InterfaceStat result = new InterfaceStat();
        
        result.name = name;
        result.duration = after.timestamp - timestamp;

        result.readBits    = after.readBits.subtract( readBits );
        result.writtenBits = after.writtenBits.subtract( writtenBits );

        return result;
        
    }

    /**
     * Compute the rate of this state over the given interval.
     */
    @Override
    public InterfaceStat rate( long interval ) {

        InterfaceStat result = new InterfaceStat();

        result.name = name;
        result.duration = duration;

        result.readBits = overInterval( readBits, interval );
        result.writtenBits = overInterval( writtenBits, interval );

        return result;
        
    }

    @Override
    public String toString() {

        StringBuilder buff = new StringBuilder();

        buff.append( String.format( "%10s %,20d %,20d",
                                    name, readBits.longValue(), writtenBits.longValue() ) );

        return buff.toString();

    }
    
}

