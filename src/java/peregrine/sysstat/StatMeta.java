
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

    @Override
    public String toString() {

        StringBuilder buff = new StringBuilder();

        for( DiskStat disk : diskStats ) {

            buff.append( disk + "\n" );

        }

        for( NetworkStat net : networkStats ) {

            buff.append( net + "\n" );

        }

        for( CPUStat cpu : cpuStats ) {

            buff.append( cpu + "\n" );

        }
        
        return buff.toString();
        
    }

}

