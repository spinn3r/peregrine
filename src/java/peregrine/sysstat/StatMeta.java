
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 * Stats for disk throughput and network throughput.
 */
public class StatMeta {

    public DiskStat disk = new DiskStat();
    public NetworkStat network = new NetworkStat();

    public List<CPUStat> processors = new ArrayList();
    
}

