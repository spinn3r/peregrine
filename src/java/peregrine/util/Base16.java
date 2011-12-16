package peregrine.util;

/**
 * Base16 - encodes 'Canonical' Base16
 */
public class Base16 {

    static final String[] hex = new String[] {
            "0", "1", "2", "3", "4", "5", "6",
            "7", "8", "9", "a", "b", "c", "d",
            "e", "f",
    };

    /**
     * @param bytes
     * @return String
     */
    public static String encode(final byte[] bytes) {

        StringBuffer base16 = new StringBuffer( bytes.length );

        for( int i = 0; i < bytes.length; i++) {
            base16.append(hex[(bytes[i] >>4) & 15]);
            base16.append(hex[(bytes[i]) & 15]);
        }
        return base16.toString();
    }

}

