package peregrine.config;

import peregrine.BaseTestWithMultipleDaemons;
import peregrine.config.partitioner.*;
import peregrine.values.*;

public class TestRangeRouter extends BaseTestWithMultipleDaemons {

	public TestRangeRouter() {
		super(5, 2, 51);
	}

	public void test1() {

		RangePartitioner router = new RangePartitioner();
		router.init(config);
		
	    for( int i = 0; i < 255; ++i ) {
	    	byte[] key = new byte[8];
	    	key[0] = (byte)i;
	    	
			Partition result = router.partition( StructReaders.wrap( key ) );
			
			System.out.printf( "result: %s\n", result );
			assertEquals( i , result.getId() );
			
	    }
		

	}
	
	public static void main(String[] args) throws Exception {

        runTests();
		
	}

}
