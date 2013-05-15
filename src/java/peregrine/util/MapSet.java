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
package peregrine.util;

import java.util.*;
import java.util.concurrent.*;

/**
 * A MapList is a K->List<V> structure where every key is a list of values.
 *
 */
public class MapSet<T,V> {

	protected ConcurrentHashMap<T,Set<V>> map = new ConcurrentHashMap();
    
    public void put( T key, V value ) {

        Set<V> set = map.get( key );

        if ( set == null ) {
            set = new ConcurrentSkipListSet();
            map.putIfAbsent( key, set );
            set = map.get( key );
        }

        set.add( value );
        
    }

    public void remove( T key, V value ) {

        Set<V> set = map.get( key );

        if ( set != null ) {
            set.remove( value );
        }
        
    }
    
    public Set<V> get( T key ) {
        return map.get( key );
    }

    public boolean contains( T key ) {
        return map.containsKey( key );
    }
    
}
