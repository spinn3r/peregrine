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

/**
 * A record is an entry in the database representated as both a key and a value.
 */
public class Record implements KeyValuePair {

    private StructReader key = null;
    private StructReader value = null;

    public Record() { }
    
    public Record( StructReader key, StructReader value ) {
        this.key = key;
        this.value = value;
    }

    @Override
    public StructReader getKey() {
        return key;
    }

    @Override
    public StructReader getValue() {
        return value;
    }

    public void setKey( StructReader key ) {
        this.key = key;
    }

    public void setValue( StructReader value ) {
        this.value = value;
    }

}
