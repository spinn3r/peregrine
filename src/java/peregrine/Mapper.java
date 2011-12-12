package peregrine;

import peregrine.map.BaseMapper;

/**
 * A map task implementation which maps keys to values and then emits them.
 */
public class Mapper extends BaseMapper {

    public void map( byte[] key, byte[] value ) {
        emit( key, value );
    }

}
