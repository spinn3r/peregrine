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

    public static int COLOR_BLACK    = 0x00;
    public static int COLOR_RED      = 0x01;
    public static int COLOR_GREEN    = 0x02;
    public static int COLOR_YELLOW   = 0x03;
    public static int COLOR_BLUE     = 0x04;
    public static int COLOR_MAGENTA  = 0x05;
    public static int COLOR_CYAN     = 0x06;
    public static int COLOR_WHITE    = 0x07;
    
    public static int A_BOLD = 0x00002000; /* Added characters are bold. */

    public static native int erase();
    public static native int clear();
    public static native int initscr();
    public static native int cbreak();
    public static native int nocbreak();
    public static native int echo();
    public static native int noecho();
    public static native int refresh();
    public static native int mvaddstr(int y, int x, String str);

    public static native int attron( int attrs );
    public static native int attroff( int attrs );

    public static native int endwin();
    public static native int start_color();
    public static native boolean has_colors();
    public static native int use_default_colors();

    public static void init() {

        initscr();
        start_color();
        cbreak();
        noecho();

        clear();
        erase();

        refresh();

    }

    public static void term() {

        nocbreak();
        echo();
        endwin();

    }
    
    static {
        Native.register( "curses" );
    }

    public static void main( String[] args ) throws Exception {

        initscr();
        start_color();
        cbreak();
        noecho();

        use_default_colors();
        
        clear();
        erase();

        if ( has_colors() ) {
            attron( A_BOLD );

            mvaddstr( 0, 2, "hello (r):" );
            mvaddstr( 1, 2, "cat (r):" );

        } else {
            mvaddstr( 0, 2, "hello:" );
            mvaddstr( 1, 2, "cat:" );

        }

        if ( has_colors() ) {
            attron( A_BOLD );
        }

        mvaddstr( 0, 20, "world" );
        mvaddstr( 1, 20, "dog" );

        refresh();

        Thread.sleep( 2000L );
        
        nocbreak();
        echo();
        endwin();
        
    }
                            
}
