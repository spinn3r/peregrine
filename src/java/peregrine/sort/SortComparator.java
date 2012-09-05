/*
 * Copyright 2011-2012 Kevin A. Burton
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
package peregrine.sort;

import java.io.*;

import peregrine.*;
import peregrine.io.chunk.*;
import peregrine.reduce.*;
import peregrine.util.*;

/**
 * <p>
 * Used by clients which require custom sorting.  This allows us to sort by
 * fields in the value or in the key.
 *
 * <p>
 * When using a value which is NOT globally unique the key should also be
 * specified which would make it unique.
 */
public interface SortComparator extends ReduceComparator {

    /**
     * <q>Compares its two arguments for order. Returns a negative integer,
     * zero, or a positive integer as the first argument is less than, equal to,
     * or greater than the second.</q>
     */
    @Override
    public int compare( KeyValuePair pair0 , KeyValuePair pair1 );

    /**
     * <p>
     * Given a key and value, compute a sort key.
     *
     * <p> A sort key is a key used to globally sort an entry in the a sort job.
     * This is the main column (or columns) from the value you want to sort
     * combined with the hashcode/key that identifies that item.
     *
     * <p> The hashcode is required for pathological cases where all the values
     * are identical so that we have globally unique keys for everything.
     */
    public StructReader getSortKey( StructReader key, StructReader value );
    
}