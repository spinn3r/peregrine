
package peregrine.sysstat;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.os.*;

import org.apache.log4j.xml.DOMConfigurator;

import com.spinn3r.log5j.Logger;

public class Main {

    public static void main( String[] args ) throws Exception {

        DOMConfigurator.configure( "conf/log4j.xml" );

        Getopt getopt = new Getopt( args );
        
        SystemProfiler profiler =
            SystemProfilerManager.getInstance( toSet( getopt.getString( "interfaces" ) ),
                                               toSet( getopt.getString( "disks" ) ),
                                               toSet( getopt.getString( "processors" ) ) );
        
        while( true ) {

            Thread.sleep( 5000L );

            StatMeta stat = profiler.rate();

            System.out.printf( "%s\n", stat );

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