package peregrine.os;

import java.io.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class Platform {

    public static boolean isLinux() {

        String os = System.getProperty("os.name").toLowerCase();

        // only attempt to run this on Platform.
        return os.contains("linux");

    }

}