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

import com.spinn3r.log5j.Logger;

/**
 * Command line main() class for the worker daemon.
 */
public class Pidfile {
	
    private static final Logger log = Logger.getLogger();

    private Config config;

    public Pidfile( Config config ) {
        this.config = config;
    }

    public int read( File file ) throws IOException {

        if ( file.exists() == false )
            return -1;

        if ( file.length() == 0 )
            return -1;
        
        FileInputStream fis = new FileInputStream( file );
        byte[] data = new byte[ (int)file.length() ]; 
        fis.read( data );
        fis.close();

        return Integer.parseInt( new String( data ) );

    }

    public int read() throws IOException {

        File file = new File( config.getRoot(), "worker.pid" );

        return read( file );
        
    }

    public void delete() throws IOException {

        File file = new File( config.getRoot(), "worker.pid" );

        if ( file.exists() && file.delete() == false )
            throw new IOException( "Unable to delete pid file: " + file.getPath() );
        
    }
        
}
    
