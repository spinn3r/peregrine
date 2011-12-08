
package peregrine.sysstat;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public class Main {

    public static void main( String[] args ) throws Exception {

        SystemProfiler profiler = SystemProfilerManager.getInstance();

        Getopt getopt = new Getopt( args );

        profiler.setInterfaces( toSet( getopt.getString( "interfaces" ) ) );
        profiler.setDisks( toSet( getopt.getString( "disks" ) ) );
        profiler.setProcessors( toSet( getopt.getString( "processors" ) ) );

        while( true ) {

            profiler.update();

            //StatMeta stat = platform.diff();
            StatMeta stat = profiler.rate();

            if ( stat != null )
                System.out.printf( "%s\n", stat );
            
            Thread.sleep( 5000L );
            
        }
        
    }

    private static Set<String> toSet( String arg ) {

        if ( arg == null )
            return null;
        
        String[] split = arg.split( "," );

        Set<String> set = new HashSet();
        
        for( String str : split ) {
            set.add( str );
        }

        return set;
        
    }
    
}