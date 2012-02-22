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
package peregrine.os;

public class PlatformException extends Exception {
    
    private int _errno = -1;

    public PlatformException() {
        this( errno.errno(), errno.strerror() );
    }

    public PlatformException( int _errno, String message ) {
        super( message );
        setErrno( _errno );
    }

    public void setErrno( int _errno ) { 
        this._errno = _errno;
    }

    public int getErrno() { 
        return this._errno;
    }

}