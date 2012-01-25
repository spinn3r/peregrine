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
package peregrine.task;

import java.util.*;

import peregrine.config.*;
import peregrine.io.driver.*;
import peregrine.io.driver.broadcast.*;
import peregrine.io.driver.file.*;
import peregrine.io.driver.shuffle.*;

/**
 * Represents work to the peregrine IO system.  
 */
public final class Work implements Comparable<Work> {
    
    private Host host = null;
    
    private int priority = 0;

    protected List<WorkReference> references = new ArrayList();

    public Work() { }

    public Work( List<String> paths ) {

        for( String path : paths ) {

            if ( path.contains( ":" ) ) {

                String scheme = path.split( ":" )[0];

                IODriver driver = IODriverRegistry.getInstance( scheme );
                
                // see if it is registered as a driver.
                if ( driver != null ) {
                    add( driver.getWorkReference( path ) );
                }

            }

        }
        
    }
    
    public Work( WorkReference... refs ) {
       add( refs );
    }

    public Work( WorkReference ref ) {
        add( ref );
    }

    public Work( WorkReference ref, int priority ) {
        add( ref );
        this.priority = priority;
    }
        
    public Work add( WorkReference ref ) {
        this.references.add( ref );
        return this;
    }

    public void add( WorkReference... refs ) {
    	add( Arrays.asList( refs ) );
    }
    
    public void add( List<WorkReference> refs ) {
    	for( WorkReference ref : refs ) {
    		add( ref );
    	}
    }
   
    public void merge( Work work ) {
    	add( work.references );
    }
    
    public List<WorkReference> getReferences() {
        return references;
    }
    
    public int size() {
    	return references.size();    	
    }

    /**
     * The priority for this unit of work vs other units of work on the same
     * host.
     */
    public int getPriority() { 
        return this.priority;
    }

    public void setPriority( int priority ) { 
        this.priority = priority;
    }

    /**
     * Return the host responsible for performing this work.
     */
    public Host getHost() { 
        return this.host;
    }

    public void setHost( Host host ) { 
        this.host = host;
    }

    @Override
    public String toString() {
        return references.toString();
    }
    
    @Override
    public int hashCode() {
    	return references.hashCode();
    }
    
    @Override
    public boolean equals( Object obj ) {
    	
    	if ( obj instanceof Work ) {
       		Work work = (Work)obj;    		
    		return references.equals( work.references );	
    	}
    	
    	return false;
    	
    }

    @Override
    public int compareTo( Work w ) {
        return priority - w.priority;
    }

}
