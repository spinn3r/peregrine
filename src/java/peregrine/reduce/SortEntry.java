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
package peregrine.reduce;

import com.spinn3r.log5j.Logger;

import java.util.*;

import peregrine.*;

/**
 * When merging, keep track of each key and the values it supports.
 */
public final class SortEntry {

    private static final Logger log = Logger.getLogger();
    
    public StructReader key;
    public byte[] keyAsByteArray;
    
    private List<StructReader> values = new ArrayList();

    public SortEntry( StructReader key, StructReader first ) {
    	this(key);
    	addValue( first );
    }

    public SortEntry( StructReader key, List<StructReader> values ) {
    	this(key);
    	this.values = values;
    }

    public SortEntry( StructReader key ) {
        this.key = key;
        this.keyAsByteArray = key.toByteArray();
    }

    public void addValue( StructReader value ) {
        this.values.add( value );
    }

    public void addValues( List<StructReader> _values ) {
        this.values.addAll( _values );
    }
    
    public List<StructReader> getValues() {
        return this.values;
    }
    
}

