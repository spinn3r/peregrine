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
import peregrine.io.*;
import peregrine.task.*;

/**
 * Take a key and list of values, and reduce them and emit result.
 */
public class Reducer extends BaseJobDelegate {

    public void reduce( StructReader key, List<StructReader> values ) {

        if( values.size() == 1 ) {
            emit( key, values.get( 0 ) );
        } else if ( values.size() > 1 ) {
            emit( key, StructReaders.wrap( values ) );
        }

    }

}
