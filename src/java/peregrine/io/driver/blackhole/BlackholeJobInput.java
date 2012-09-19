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
package peregrine.io.driver.blackhole;

import java.io.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.driver.blackhole.*;

/**
 * @see BlackholeInputReference
 */
public class BlackholeJobInput extends BaseJobInput implements JobInput {

    @Override
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public void next() throws IOException {
        //noop
    }
    
    @Override
    public StructReader key() throws IOException {
        return null;
    }

    @Override
    public StructReader value() throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {
        //noop
    }

    @Override
    public int size() {
        return 0;
    }

}
