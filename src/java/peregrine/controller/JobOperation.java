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
package peregrine.controller;

import com.spinn3r.log5j.*;

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.util.*;

/**
 * The basic / low-level Peregrine operation.  Usually map/reduce/merge.
 * Peregrine supports higher level composite operations like touch/sort which
 * are really just batches of lower level operations.  
 */
public class JobOperation {

    public static String MAP     = "map";
    
    public static String MERGE   = "merge";
    
    public static String REDUCE  = "reduce";

}
