package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;
import java.lang.reflect.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;
import maprunner.io.*;

public class Test3 {

    public static void main( String... args ) throws Exception {

        PriorityQueue<Integer> queue = new PriorityQueue();

        queue.add( 1 );
        queue.add( 1 );

        System.out.printf( "%s \n", queue.poll() );
        System.out.printf( "%s \n", queue.poll() );
        
    }

}
