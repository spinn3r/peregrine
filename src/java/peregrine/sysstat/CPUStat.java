
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class CPUStat {

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

    public void diff( CPUStat before, CPUStat after ) {

        user    = after.user.subtract( before.user );
        nice    = after.nice.subtract( before.nice );
        system  = after.system.subtract( before.system );
        idle    = after.idle.subtract( before.idle );
        iowait  = after.iowait.subtract( before.iowait );
        irq     = after.irq.subtract( before.irq );
        softirq = after.softirq.subtract( before.softirq );

        init();
        
    }

}

