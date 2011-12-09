
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

        StringBuilder buff = new StringBuilder();
        
        for( Object arg : args ) {

            if ( "\n".equals( arg ) ) {
                buff.append( arg );
            } else {

                if( arg instanceof Double )
                    buff.append( format( (Double)arg ) );
                else if( arg instanceof Integer )
                    buff.append( format( (Integer)arg ) );
                else if( arg instanceof Long )
                    buff.append( format( (Long)arg ) );
                else 
                    buff.append( format( arg ) );

            }
            
        }

        return buff.toString();
        
    }

    protected String format( Double obj ) {
        return String.format( "%,15.2f ", obj.doubleValue() );
    }

    protected String format( Long obj ) {
        return String.format( "%,15d ", obj.longValue() );
    }

    protected String format( Integer obj ) {
        return String.format( "%,15d ", obj.intValue() );
    }

    protected String format( Object obj ) {
        return String.format( "%15s ", obj.toString() );
    }

}
