
package peregrine.sysstat;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public class Main {

    public static void main( String[] args ) throws Exception {

        String dev = args[1];
        
        LinuxPlatform platform = new LinuxPlatform( dev );

        while( true ) {

            platform.update();

            Thread.sleep( 1000L );
            
        }
        
    }

}