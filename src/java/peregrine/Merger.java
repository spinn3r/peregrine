package peregrine;

import java.util.*;
import peregrine.map.BaseMapper;
import peregrine.values.*;

/**
 * 
 * A merger implementation that performs a merge against two data sets.
 *
 * <p>
 * Both datasets must be sorted by key.  An extract can do this with the 
 * ExtractWriter if the original input was sorted.  If not, you will need to run 
 * an identity map job to re-emit the data and reduce it so that 
 * the data is sorted by key.  If the data is not sorted you will have undefined 
 * behavior.
 * 
 * @author burton@spinn3r.com
 *
 */
public class Merger extends BaseMapper {
	
	/**
	 * Run a merge over the given data.  Note the merge method is different 
	 * from that of Mapper in that only one value is provided. 
	 * 
	 * @param key
	 * @param values
	 * @see Mapper#map(byte[], byte[])
	 */
    public void merge( StructReader key, List<StructReader> values ) {}

}
