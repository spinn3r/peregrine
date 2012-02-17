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
package peregrine.util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Simpilar to MarkMap but we keep track of and incrementing counter per key.
 */
public class IncrMap<T> {

    ConcurrentHashMap<T,AtomicInteger> map = new ConcurrentHashMap();

    public IncrMap() {}

    public IncrMap( Set<T> in ) {

        for( T key : in ) {
            init( key );
        }
        
    }

    public IncrMap( List<T> in ) {

        for( T key : in ) {
            init( key );
        }
        
    }

    public void init( T key ) {
        map.putIfAbsent( key, new AtomicInteger() );
    }
    
    public void incr( T key ) {

        if ( map.containsKey( key ) == false ) {
            init( key );
        }
        
        map.get( key ).getAndIncrement();
        
    }

    public void decr( T key ) {
        map.get( key ).getAndDecrement();
    }

    public int get( T key ) {
        return map.get( key ).get();
    }

    public void set( T key, int value ) {
        map.get( key ).set( value );
    }

    public int size() {
        return map.size();
    }
    
}
