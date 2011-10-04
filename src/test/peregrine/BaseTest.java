package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.shuffle.*;
import peregrine.io.*;
import peregrine.perf.*;

public abstract class BaseTest extends junit.framework.TestCase {

    public void setUp() {
        System.out.printf( "Cleaning up PDFS_ROOT: %s\n", Config.PDFS_ROOT );
        DiskPerf.remove( Config.PDFS_ROOT );
    }

    public void tearDown() {
        System.out.printf( "Cleaning up PDFS_ROOT: %s\n", Config.PDFS_ROOT );
        DiskPerf.remove( Config.PDFS_ROOT );
    }

}
