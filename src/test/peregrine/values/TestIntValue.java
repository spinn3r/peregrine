package peregrine.values;

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