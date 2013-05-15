/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

        if ( ! isZero( total_jiffies ) ) {
            active = active_jiffies.divide( total_jiffies, 2, RoundingMode.CEILING ).doubleValue() * 100;
        }

    }

    @Override
    public ProcessorStat diff( ProcessorStat after ) {

        ProcessorStat result = new ProcessorStat();
        
        result.name = name;
        result.duration = after.timestamp - timestamp;
        
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

        result.name = name;
        result.duration = interval;

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

        buff.append( format( name, active ) );

        return buff.toString();

    }
    
}

