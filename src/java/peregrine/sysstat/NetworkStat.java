
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class NetworkStat {

    BigDecimal readBytes = new BigDecimal();
    BigDecimal writtenBytes = new BigDecimal();

    public void diff( NetworkStat before, NetworkStat after ) {
        
        readBytes    = after.readBytes.subtract( before.readBytes );
        writtenBytes = after.writtenBytes.subtract( before.writtenBytes );

    }

}

