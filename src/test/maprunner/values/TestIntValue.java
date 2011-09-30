package maprunner.values;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;
import maprunner.io.*;

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