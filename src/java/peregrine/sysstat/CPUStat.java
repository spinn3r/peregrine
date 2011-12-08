
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class CPUStat implements Diffable<CPUStat> {

    String name;
    
    BigDecimal user     = new BigDecimal( 0 );
    BigDecimal nice     = new BigDecimal( 0 );
    BigDecimal system   = new BigDecimal( 0 );
    BigDecimal idle     = new BigDecimal( 0 );
    BigDecimal iowait   = new BigDecimal( 0 );
    BigDecimal irq      = new BigDecimal( 0 );
    BigDecimal softirq  = new BigDecimal( 0 );

    BigDecimal active_jiffies  = new BigDecimal( 0 );
    BigDecimal total_jiffies   = new BigDecimal( 0 );

    /**
     * Percentage of idle CPU from 0 to 100.
     */
    public double active = 0;

    public void init() {

        active_jiffies =
            active_jiffies.add( user )
                          .add( nice )
                          .add( system )
            ;

        total_jiffies =
            active_jiffies.add( idle )
                          .add( iowait )
                          .add( irq )
                          .add( softirq )
            ;

        active = active_jiffies.divide( total_jiffies, 2, RoundingMode.CEILING ).doubleValue() * 100;

    }

    public CPUStat diff( CPUStat after ) {

        CPUStat result = new CPUStat();
        
        result.name = name;
        
        result.user    = after.user.subtract( user );
        result.nice    = after.nice.subtract( nice );
        result.system  = after.system.subtract( system );
        result.idle    = after.idle.subtract( idle );
        result.iowait  = after.iowait.subtract( iowait );
        result.irq     = after.irq.subtract( irq );
        result.softirq = after.softirq.subtract( softirq );

        result.init();

        return result;
        
    }

    @Override
    public String toString() {
        return String.format( "%10s %,20.2f", name, active );

    }
    
}

