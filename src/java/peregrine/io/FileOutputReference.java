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
package peregrine.io;

public final class FileOutputReference implements OutputReference {
    
    private boolean append = false;

    private String path;

    public FileOutputReference( String path ) {
        this( path, false );
    }
    
    public FileOutputReference( String path, boolean append ) {
        this.path = path;
        this.append = append;
    }

    public String getPath() {
        return this.path;
    }

    public boolean getAppend() { 
        return this.append;
    }

    @Override
    public String toString() {
        return String.format( "%s:%s:%s", getScheme(), getPath(), append );
    }

    @Override
	public String getScheme() {
    	return "file";
    }    
    
}
    
