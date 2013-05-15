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
package peregrine.io;

import java.util.*;

import peregrine.config.*;
import peregrine.io.driver.*;
import peregrine.io.driver.broadcast.*;
import peregrine.io.driver.file.*;
import peregrine.io.driver.shuffle.*;

/**
 * Represents input to the peregrine IO system.  Note that having <b>no</b>
 * input is acceptable as this is a valid way to write nothing to a file.
 *
 */
public final class Input {

    private List<InputReference> references = new ArrayList();

    public Input() { }

    public Input( List<String> paths ) {

        for( String path : paths ) {

            if ( path.contains( ":" ) ) {

                String scheme = path.split( ":" )[0];

                IODriver driver = IODriverRegistry.getInstance( scheme );
                
                // see if it is registered as a driver.
                if ( driver != null ) {
                    add( driver.getInputReference( path ) );
                }

            } else { 
                add( new FileInputReference( path ) );
            }

        }
        
    }
    
    public Input( String... paths ) {
        this( Arrays.asList( paths ) );
    }

    protected Input( InputReference... refs ) {
        for( InputReference ref : refs )
            add( ref );
    }

    protected Input( InputReference ref ) {
        add( ref );
    }
    
    public Input add( InputReference ref ) {
        this.references.add( ref );
        return this;
    }

    public List<InputReference> getReferences() {
        return references;
    }
    
    public int size() {
    	return references.size();    	
    }
    
    @Override
    public String toString() {
        return references.toString();
    }
    
}
