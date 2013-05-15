/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

        System.out.printf( "%s\n", profiler.update() );

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
