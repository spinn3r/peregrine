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

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public class curses {

    public static native int erase();
    public static native int clear();
    public static native int initscr();
    public static native int cbreak();
    public static native int nocbreak();
    public static native int echo();
    public static native int noecho();
    public static native int refresh();
    public static native int mvaddstr(int y, int x, String str);
    
    public static native int endwin();

    static {
        Native.register( "curses" );
    }

    public static void main( String[] args ) throws Exception {

        initscr();
        cbreak();
        noecho();

        clear();
        erase();

        mvaddstr( 0, 2, "hello world" );
        mvaddstr( 1, 2, "second column" );
        
        refresh();

        Thread.sleep( 2000L );
        
        nocbreak();
        echo();
        endwin();
        
    }
                            
}
