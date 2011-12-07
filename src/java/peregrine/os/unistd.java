package peregrine.os;

import java.io.*;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public class unistd {

    private static InterfaceDelegate delegate
        = (InterfaceDelegate)Native.loadLibrary( "c", InterfaceDelegate.class); 

    public static void sync() {
        delegate.sync();
    }

    interface InterfaceDelegate extends Library {
        void sync();
    }

}
