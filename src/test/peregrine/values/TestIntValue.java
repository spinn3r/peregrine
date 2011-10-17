package peregrine.values;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.reduce.*;
import peregrine.io.*;

public class TestIntValue extends junit.framework.TestCase {

    public void test1() {

        for( int i = 0; i < 500; ++i ) {

            IntValue iv = new IntValue( i );

            byte[] data = iv.toBytes();

            iv = new IntValue();
            iv.fromBytes( data );

            assertEquals( iv.value, i );
            
        }
        
    }
    
}