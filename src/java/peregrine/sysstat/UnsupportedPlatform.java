
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 * Null object platform.  When the platform is unsupported we just silently do
 * nothing which requires no special handling by the caller.
 */ 
public class UnsupportedPlatform extends BaseSystemProfiler {

    public StatMeta update() {
        return new StatMeta();
    }

}