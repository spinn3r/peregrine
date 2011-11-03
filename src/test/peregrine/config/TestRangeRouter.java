package peregrine.config;

import peregrine.BaseTestWithMultipleDaemons;

public class TestRangeRouter extends BaseTestWithMultipleDaemons {

	public TestRangeRouter() {
		super(5, 2, 51);
	}

	public void test1() {

		RangePartitionRouter router = new RangePartitionRouter();
		router.init(config);
		
	    for( int i = 0; i < 255; ++i ) {
	    	byte[] key = new byte[8];
	    	key[0] = (byte)i;

			Partition result = router.route( key );
			
			System.out.printf( "result: %s\n", result );
			assertEquals( i , result.getId() );
			
	    }
		

	}
	
	public static void main(String[] args) throws Exception {

        runTests();
		
	}

}
