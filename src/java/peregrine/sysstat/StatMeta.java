
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

        buff.append( String.format( "%10s %20s", "", "  % util\n" ) );
        buff.append( String.format( "%10s %20s", "", "  ------\n" ) );

        for( CPUStat cpu : cpuStats ) {

            buff.append( cpu + "\n" );

        }

        buff.append( "\n" );

        buff.append( String.format( "%10s %20s %20s", "", "bytes read", "bytes written\n" ) );
        buff.append( String.format( "%10s %20s %20s", "", "----------", "-------------\n" ) );

        for( DiskStat disk : diskStats ) {

            buff.append( disk + "\n" );

        }

        buff.append( "\n" );

        buff.append( String.format( "%10s %20s %20s", "", "bytes rx", "bytes tx\n" ) );
        buff.append( String.format( "%10s %20s %20s", "", "--------", "--------\n" ) );

        for( NetworkStat net : networkStats ) {

            buff.append( net + "\n" );

        }

        return buff.toString();
        
    }

}

