
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

        return value.divide( new BigDecimal( duration / (double)interval ) );
        
    }

}
