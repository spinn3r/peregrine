
package peregrine.sysstat;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public class Main {

    public static void main( String[] args ) throws Exception {

        Platform platform = PlatformManager.getPlatform();

        Getopt getopt = new Getopt( args );

        platform.setInterfaces( toSet( getopt.getString( "interfaces" ) ) );
        platform.setDisks( toSet( getopt.getString( "disks" ) ) );
        platform.setProcessors( toSet( getopt.getString( "processors" ) ) );

        while( true ) {

            platform.update();

            //StatMeta stat = platform.diff();
            StatMeta stat = platform.rate();

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