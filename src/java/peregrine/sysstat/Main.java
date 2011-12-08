
package peregrine.sysstat;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public class Main {

    public static void main( String[] args ) throws Exception {

        final String _disk = args[0];
        final String _interface = args[1];

        Platform platform = PlatformManager.getPlatform();

        platform.setInterfaces( new HashSet() {{
            add( _interface );
        }} );

        platform.setDisks( new HashSet() {{
            add( _disk );
        }} );

        while( true ) {

            platform.update();

            //StatMeta stat = platform.diff();
            StatMeta stat = platform.rate();

            if ( stat != null )
                System.out.printf( "%s\n", stat );
            
            Thread.sleep( 5000L );
            
        }
        
    }

}