
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

    public static final BigDecimal ZERO = new BigDecimal( 0 );
    
    String name;

    long timestamp = System.currentTimeMillis();
    long duration  = timestamp;
    
    protected BigDecimal overInterval( BigDecimal value , long interval ) {

        double range = duration / (double)interval;

        return value.divide( new BigDecimal( range ), 2, RoundingMode.CEILING );
        
    }

    /**
     * Perform any derived computation from the metrics on this stat.
     */
    public void init() {

    }

    protected boolean isZero( BigDecimal value ) {
        return value.compareTo( ZERO ) == 0;
    }

    protected String format( Object... args ) {

        for( Object arg : args ) {
            format( arg );
        }

        return null;
        
    }

    protected String format( Double obj ) {
        return String.format( "%,15.2f", obj.doubleValue() );
    }

    protected String format( Integer obj ) {
        return String.format( "%,15d", obj.intValue() );
    }

    protected String format( Object obj ) {
        return String.format( "%15s", obj.toString() );
    }

}
