
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class BaseStat {

    String name;

    long timestamp = System.currentTimeMillis();
    long duration = timestamp;

    protected BigDecimal overInterval( BigDecimal value , long interval ) {

        double range = duration / (double)interval;

        System.out.printf( "FIXME: duration: %s\n", duration );
        System.out.printf( "FIXME: interval: %s\n", interval );
        System.out.printf( "FIXME: range: %s\n", range );

        return value.divide( new BigDecimal( range ), 2, RoundingMode.CEILING );
        
    }

}
