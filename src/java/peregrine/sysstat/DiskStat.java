
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class DiskStat {

    BigDecimal readBytes = new BigDecimal();
    BigDecimal writtenBytes = new BigDecimal();

    public void diff( DiskStat before, DiskStat after ) {
        
        readBytes    = after.readBytes.subtract( before.readBytes );
        writtenBytes = after.writtenBytes.subtract( before.writtenBytes );

    }

}

