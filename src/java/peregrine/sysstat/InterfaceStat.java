
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class InterfaceStat implements Diffable<InterfaceStat> {

    String name;
    
    BigDecimal readBytes     = new BigDecimal( 0 );
    BigDecimal writtenBytes  = new BigDecimal( 0 );

    public InterfaceStat diff( InterfaceStat after ) {

        InterfaceStat result = new InterfaceStat();
        
        result.name = name;

        result.readBytes    = after.readBytes.subtract( readBytes );
        result.writtenBytes = after.writtenBytes.subtract( writtenBytes );

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

