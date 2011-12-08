
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class ProcessorStat extends BaseStat implements Diffable<ProcessorStat> {
    
    BigDecimal user     = new BigDecimal( 0 );
    BigDecimal nice     = new BigDecimal( 0 );
    BigDecimal system   = new BigDecimal( 0 );
    BigDecimal idle     = new BigDecimal( 0 );
    BigDecimal iowait   = new BigDecimal( 0 );
    BigDecimal irq      = new BigDecimal( 0 );
    BigDecimal softirq  = new BigDecimal( 0 );

    BigDecimal active_jiffies  = new BigDecimal( 0 );
    BigDecimal total_jiffies   = new BigDecimal( 0 );

    double active = 0;
    
    /**
     * Percentage of idle CPU from 0 to 100.
     */

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

        if ( ! total_jiffies.equals( new BigDecimal( 0 ) ) ) {
            active = active_jiffies.divide( total_jiffies, 2, RoundingMode.CEILING ).doubleValue() * 100;
        }

    }

    @Override
    public ProcessorStat diff( ProcessorStat after ) {

        ProcessorStat result = new ProcessorStat();
        
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

    /**
     * Compute the rate of this state over the given interval.
     */
    @Override
    public ProcessorStat rate( long interval ) {

        ProcessorStat result = new ProcessorStat();

        result.user    = overInterval( user, interval );
        result.nice    = overInterval( nice, interval );
        result.system  = overInterval( system, interval );
        result.idle    = overInterval( idle, interval );
        result.iowait  = overInterval( iowait, interval );
        result.irq     = overInterval( irq, interval );
        result.softirq = overInterval( softirq, interval );

        result.init();

        return result;
        
    }

    @Override
    public String toString() {

        StringBuilder buff = new StringBuilder();

        buff.append( String.format( "%10s %,20.2f", name, active ) );

        return buff.toString();

    }
    
}

