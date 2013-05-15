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
package peregrine.io.util;

import java.util.*;
import java.io.*;

/**
 * Implements JDK 1.7 try-with-resources style closing for multiple Closeables /
 * Flushables with an exception that includes all exceptions.
 *
 * We use the term 'repressed' instead of 'suppressed' to allow us to compile on
 * JDK 1.7.
 *
 * @see <a href='http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html'>try-with-resources</a>
 */
public class GroupIOException extends IOException {

    List<Throwable> repressed = new ArrayList();
    
    public GroupIOException( Throwable cause ) {
        repressed.add( cause );
    }
    
    public void addRepressed( Throwable t ) {
        repressed.add( t );
    }

    public void printStackTrace( PrintStream out ) {

        for ( Throwable current : repressed ) {
            current.printStackTrace( out );
        }

        // this will print ourselves AND the cause... 
        super.printStackTrace( out );

    }

    public void printStackTrace( PrintWriter out ) {

        for ( Throwable current : repressed ) {
            current.printStackTrace( out );
        }

        // this will print ourselves AND the cause... 
        super.printStackTrace( out );

    }
    
}
    
