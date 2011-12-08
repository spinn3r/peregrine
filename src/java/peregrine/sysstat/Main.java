
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

            StatMeta stat = platform.update();

            System.out.printf( "%s\n", stat );
            
            Thread.sleep( 1000L );
            
        }
        
    }

}