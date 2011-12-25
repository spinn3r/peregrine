package peregrine;

import peregrine.map.BaseMapper;

/**
 * A map task implementation which maps keys to values and then emits them.
 */
public class Mapper extends BaseMapper {

    public void map( StructReader key, StructReader value ) {
        emit( key, value );
    }

}
