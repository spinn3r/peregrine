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
package peregrine.os.proc;

import java.io.*;
import java.util.*;

import peregrine.util.*;

import com.spinn3r.log5j.Logger;

/**
 * Linux specific functions.
 */
public class ProcessList {
    
    private List<ProcessListEntry> processes = new ArrayList();;

    private static final String NULL = new String( new char[] { (char)0 } );
    
    /**
     */
    public ProcessList() throws Exception {

        File proc = new File( "/proc" );

        if ( ! proc.exists() )
            return;
        
        for ( String current : proc.list() ) {

            if ( current.matches( "[0-9]+" ) ) {

                FileInputStream fis = null;

                try {

                    File file = new File( proc, current );
                    file = new File( file, "cmdline" );

                    fis = new FileInputStream( file );

                    byte[] data = new byte[ 1024 ];
                    int read = fis.read( data );

                    if ( read <= 0 )
                        continue;
                    
                    byte[] _data = new byte[read];
                    System.arraycopy( data, 0, _data, 0, read );
                    data = _data;
                                                     
                    String cmdline = new String( data );
                    
                    ProcessListEntry entry = new ProcessListEntry();

                    entry.setId( Integer.parseInt( current ) );
                    entry.setArguments( Strings.toList( cmdline.split( NULL ) ) );
                    
                    processes.add( entry );

                } catch ( FileNotFoundException fff ) {

                    // this is acceptable because the proc could have terminated
                    // since the first list of procs.
                    
                } finally {
                    if ( fis != null )
                        fis.close();
                }

            }
            
        }
        
    }

    public List<ProcessListEntry> getProcesses() { 
        return this.processes;
    }

    public void setProcesses( List<ProcessListEntry> processes ) { 
        this.processes = processes;
    }

    public static void main( String[] args ) throws Exception {

        ProcessList ps = new ProcessList();

        for ( ProcessListEntry proc : ps.getProcesses() ) {
            System.out.printf( "%s\n", proc );
        }
        
    }
    
}