/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.worker;

import java.io.*;
import java.util.*;

import peregrine.config.*;
import peregrine.os.*;

import org.apache.log4j.xml.DOMConfigurator;

/**
 * Initializer for bringing up the system. 
 */
public final class Initializer {

    public static void doInitLogger( Config config ) {

        System.setProperty( "peregrine.host", "" + config.getHost() );

        DOMConfigurator.configure( "conf/log4j.xml" );

    }

    public static void doWritePidfile( Config config ) throws IOException {

        File file = new File( config.getRoot(), "worker.pid" );
        FileOutputStream fos = new FileOutputStream( file );
        fos.write( String.format( "%s", unistd.getpid() ).getBytes() );
        fos.close();

    }

}