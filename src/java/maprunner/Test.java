package maprunner;

import java.io.*;
import java.util.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;

public class Test {

    public static void main( String[] args ) throws Exception {

        VarintWriter writer = new VarintWriter();

        for( int i = 0; i < 200; ++i ) {
            byte[] data = writer.write( i );

            //data = data >> 7;
            
            System.out.printf( "i=%d , len: %d , test: %d\n" , i, data.length, (data[0] >> 7) );

        }
        
    }

}