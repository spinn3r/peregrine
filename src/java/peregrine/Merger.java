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
package peregrine;

import java.util.*;
import peregrine.map.BaseMapper;

/**
 * 
 * A merger implementation that performs a merge against two data sets.
 *
 * <p>
 * Both datasets must be sorted by key.  An extract can do this with the 
 * ExtractWriter if the original input was sorted.  If not, you will need to run 
 * an identity map job to re-emit the data and reduce it so that 
 * the data is sorted by key.  If the data is not sorted you will have undefined 
 * behavior.
 * 
 * @author burton@spinn3r.com
 *
 */
public class Merger extends BaseMapper {
	
	/**
	 * Run a merge over the given data.  Note the merge method is different 
	 * from that of Mapper in that only one value is provided. 
	 * 
	 * @param key
	 * @param values
	 * @see Mapper#map(StructReader, StructReader)
	 */
    public void merge( StructReader key, List<StructReader> values ) {}

}
