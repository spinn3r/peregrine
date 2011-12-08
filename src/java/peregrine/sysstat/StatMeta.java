
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

    public List<DiskStat>     diskStats      = new ArrayList();
    public List<NetworkStat>  networkStats   = new ArrayList();
    public List<CPUStat>      cpuStats       = new ArrayList();
    
}

