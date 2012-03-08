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

import java.io.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class Platform {

    public static String getOS() {
        return System.getProperty("os.name").toLowerCase();
    }
    
    public static boolean isLinux() {
        return getOS().contains("linux");
    }

    public static boolean isDarwin() {
        return getOS().contains("darwin");
    }

}
