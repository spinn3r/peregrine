package maprunner.test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class DiskPerf {

    public static void sync() throws Exception {

        System.out.printf( "sync..." );
        
        int result = Runtime.getRuntime().exec( "sync") .waitFor();

        if ( result != 0 )
            throw new Exception( "sync failed" );

        System.out.printf( "done\n" );
        
    }

    
}