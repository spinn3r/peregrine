/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.os;

import com.sun.jna.Library;
import com.sun.jna.Native;

public class errno {

    private static InterfaceDelegate delegate
        = (InterfaceDelegate)Native.loadLibrary( "c", InterfaceDelegate.class); 

    // http://linux.die.net/include/asm/errno.h	
    
    public static final int  EPERM        =  1  ; /* Operation not permitted */
    public static final int  ENOENT       =  2  ; /* No such file or directory */
    public static final int  ESRCH        =  3  ; /* No such process */
    public static final int  EINTR        =  4  ; /* Interrupted system call */
    public static final int  EIO          =  5  ; /* I/O error */
    public static final int  ENXIO        =  6  ; /* No such device or address */
    public static final int  E2BIG        =  7  ; /* Arg list too long */
    public static final int  ENOEXEC      =  8  ; /* Exec format error */
    public static final int  EBADF        =  9  ; /* Bad file number */
    public static final int  ECHILD       =  10  ; /* No child processes */
    public static final int  EAGAIN       =  11  ; /* Try again */
    public static final int  ENOMEM       =  12  ; /* Out of memory */
    public static final int  EACCES       =  13  ; /* Permission denied */
    public static final int  EFAULT       =  14  ; /* Bad address */
    public static final int  ENOTBLK      =  15  ; /* Block device required */
    public static final int  EBUSY        =  16  ; /* Device or resource busy */
    public static final int  EEXIST       =  17  ; /* File exists */
    public static final int  EXDEV        =  18  ; /* Cross-device link */
    public static final int  ENODEV       =  19  ; /* No such device */
    public static final int  ENOTDIR      =  20  ; /* Not a directory */
    public static final int  EISDIR       =  21  ; /* Is a directory */
    public static final int  EINVAL       =  22  ; /* Invalid argument */
    public static final int  ENFILE       =  23  ; /* File table overflow */
    public static final int  EMFILE       =  24  ; /* Too many open files */
    public static final int  ENOTTY       =  25  ; /* Not a typewriter */
    public static final int  ETXTBSY      =  26  ; /* Text file busy */
    public static final int  EFBIG        =  27  ; /* File too large */
    public static final int  ENOSPC       =  28  ; /* No space left on device */
    public static final int  ESPIPE       =  29  ; /* Illegal seek */
    public static final int  EROFS        =  30  ; /* Read-only file system */
    public static final int  EMLINK       =  31  ; /* Too many links */
    public static final int  EPIPE        =  32  ; /* Broken pipe */
    public static final int  EDOM         =  33  ; /* Math argument out of domain of func */
    public static final int  ERANGE       =  34  ; /* Math result not representable */
    public static final int  EDEADLK      =  35  ; /* Resource deadlock would occur */
    public static final int  ENAMETOOLONG =  36  ; /* File name too long */
    public static final int  ENOLCK       =  37  ; /* No record locks available */
    public static final int  ENOSYS       =  38  ; /* Function not implemented */
    public static final int  ENOTEMPTY    =  39  ; /* Directory not empty */
    public static final int  ELOOP        =  40  ; /* Too many symbolic links encountered */
    public static final int  EWOULDBLOCK  = EAGAIN  ; /* Operation would block */
    public static final int  ENOMSG       =  42  ; /* No message of desired type */
    public static final int  EIDRM        =  43  ; /* Identifier removed */
    public static final int  ECHRNG       =  44  ; /* Channel number out of range */
    public static final int  EL2NSYNC     =  45  ; /* Level 2 not synchronized */
    public static final int  EL3HLT       =  46  ; /* Level 3 halted */
    public static final int  EL3RST       =  47  ; /* Level 3 reset */
    public static final int  ELNRNG       =  48  ; /* Link number out of range */
    public static final int  EUNATCH      =  49  ; /* Protocol driver not attached */
    public static final int  ENOCSI       =  50  ; /* No CSI structure available */
    public static final int  EL2HLT       =  51  ; /* Level 2 halted */
    public static final int  EBADE        =  52  ; /* Invalid exchange */
    public static final int  EBADR        =  53  ; /* Invalid request descriptor */
    public static final int  EXFULL       =  54  ; /* Exchange full */
    public static final int  ENOANO       =  55  ; /* No anode */
    public static final int  EBADRQC      =  56  ; /* Invalid request code */
    public static final int  EBADSLT      =  57  ; /* Invalid slot */

    public static final int  EDEADLOCK    = EDEADLK;

    public static final int  EBFONT      =  59  ; /* Bad font file format */
    public static final int  ENOSTR      =  60  ; /* Device not a stream */
    public static final int  ENODATA     =  61  ; /* No data available */
    public static final int  ETIME       =  62  ; /* Timer expired */
    public static final int  ENOSR       =  63  ; /* Out of streams resources */
    public static final int  ENONET      =  64  ; /* Machine is not on the network */
    public static final int  ENOPKG      =  65  ; /* Package not installed */
    public static final int  EREMOTE     =  66  ; /* Object is remote */
    public static final int  ENOLINK     =  67  ; /* Link has been severed */
    public static final int  EADV        =  68  ; /* Advertise error */
    public static final int  ESRMNT      =  69  ; /* Srmount error */
    public static final int  ECOMM       =  70  ; /* Communication error on send */
    public static final int  EPROTO      =  71  ; /* Protocol error */
    public static final int  EMULTIHOP   =  72  ; /* Multihop attempted */
    public static final int  EDOTDOT     =  73  ; /* RFS specific error */
    public static final int  EBADMSG     =  74  ; /* Not a data message */
    public static final int  EOVERFLOW   =  75  ; /* Value too large for defined data type */
    public static final int  ENOTUNIQ    =  76  ; /* Name not unique on network */
    public static final int  EBADFD      =  77  ; /* File descriptor in bad state */
    public static final int  EREMCHG     =  78  ; /* Remote address changed */
    public static final int  ELIBACC     =  79  ; /* Can not access a needed shared library */
    public static final int  ELIBBAD     =  80  ; /* Accessing a corrupted shared library */
    public static final int  ELIBSCN     =  81  ; /* .lib section in a.out corrupted */
    public static final int  ELIBMAX     =  82  ; /* Attempting to link in too many shared libraries */
    public static final int  ELIBEXEC    =  83  ; /* Cannot exec a shared library directly */
    public static final int  EILSEQ      =  84  ; /* Illegal byte sequence */
    public static final int  ERESTART    =  85  ; /* Interrupted system call should be restarted */
    public static final int  ESTRPIPE    =  86  ; /* Streams pipe error */
    public static final int  EUSERS      =  87  ; /* Too many users */
    public static final int  ENOTSOCK    =  88  ; /* Socket operation on non-socket */
    public static final int  EDESTADDRREQ    =  89  ; /* Destination address required */
    public static final int  EMSGSIZE    =  90  ; /* Message too long */
    public static final int  EPROTOTYPE  =  91  ; /* Protocol wrong type for socket */
    public static final int  ENOPROTOOPT =  92  ; /* Protocol not available */
    public static final int  EPROTONOSUPPORT =  93  ; /* Protocol not supported */
    public static final int  ESOCKTNOSUPPORT =  94  ; /* Socket type not supported */
    public static final int  EOPNOTSUPP  =  95  ; /* Operation not supported on transport endpoint */
    public static final int  EPFNOSUPPORT    =  96  ; /* Protocol family not supported */
    public static final int  EAFNOSUPPORT    =  97  ; /* Address family not supported by protocol */
    public static final int  EADDRINUSE  =  98  ; /* Address already in use */
    public static final int  EADDRNOTAVAIL   =  99  ; /* Cannot assign requested address */
    public static final int  ENETDOWN    =  100 ; /* Network is down */
    public static final int  ENETUNREACH =  101 ; /* Network is unreachable */
    public static final int  ENETRESET   =  102 ; /* Network dropped connection because of reset */
    public static final int  ECONNABORTED    =  103 ; /* Software caused connection abort */
    public static final int  ECONNRESET  =  104 ; /* Connection reset by peer */
    public static final int  ENOBUFS     =  105 ; /* No buffer space available */
    public static final int  EISCONN     =  106 ; /* Transport endpoint is already connected */
    public static final int  ENOTCONN    =  107 ; /* Transport endpoint is not connected */
    public static final int  ESHUTDOWN   =  108 ; /* Cannot send after transport endpoint shutdown */
    public static final int  ETOOMANYREFS    =  109 ; /* Too many references: cannot splice */
    public static final int  ETIMEDOUT   =  110 ; /* Connection timed out */
    public static final int  ECONNREFUSED    =  111 ; /* Connection refused */
    public static final int  EHOSTDOWN   =  112 ; /* Host is down */
    public static final int  EHOSTUNREACH    =  113 ; /* No route to host */
    public static final int  EALREADY    =  114 ; /* Operation already in progress */
    public static final int  EINPROGRESS =  115 ; /* Operation now in progress */
    public static final int  ESTALE      =  116 ; /* Stale NFS file handle */
    public static final int  EUCLEAN     =  117 ; /* Structure needs cleaning */
    public static final int  ENOTNAM     =  118 ; /* Not a XENIX named type file */
    public static final int  ENAVAIL     =  119 ; /* No XENIX semaphores available */
    public static final int  EISNAM      =  120 ; /* Is a named type file */
    public static final int  EREMOTEIO   =  121 ; /* Remote I/O error */
    public static final int  EDQUOT      =  122 ; /* Quota exceeded */

    public static final int  ENOMEDIUM   =  123 ; /* No medium found */
    public static final int  EMEDIUMTYPE =  124 ; /* Wrong medium type */
    public static final int  ECANCELED   =  125 ; /* Operation Cancelled */
    public static final int  ENOKEY      =  126 ; /* Required key not available */
    public static final int  EKEYEXPIRED =  127 ; /* Key has expired */
    public static final int  EKEYREVOKED =  128 ; /* Key has been revoked */
    public static final int  EKEYREJECTED    =  129 ; /* Key was rejected by service */

    /**
     * The routine perror() produces a message on the standard error output,
     * describing the last error encountered during a call to a system or
     * library function. First (if s is not NULL and *s is not a null byte
     * ('\0')) the argument string s is printed, followed by a colon and a
     * blank. Then the message and a new-line.
     * 
     * To be of most use, the argument string should include the name of the
     * function that incurred the error. The error number is taken from the
     * external variable errno, which is set when errors occur but not cleared
     * when non-erroneous calls are made.
     * 
     * The global error list sys_errlist[] indexed by errno can be used to
     * obtain the error message without the newline. The largest message number
     * provided in the table is sys_nerr -1. Be careful when directly accessing
     * this list because new error values may not have been added to
     * sys_errlist[].
     * 
     * When a system call fails, it usually returns -1 and sets the variable
     * errno to a value describing what went wrong. (These values can be found
     * in <errno.h>.) Many library functions do likewise. The function perror()
     * serves to translate this error code into human-readable form. Note that
     * errno is undefined after a successful library call: this call may well
     * change this variable, even though it succeeds, for example because it
     * internally used some other library function that failed. Thus, if a
     * failing call is not immediately followed by a call to perror(), the value
     * of errno should be saved.
     */
    public static int perror( String s ) {
        return delegate.perror( s );
    }

    /**
     * The strerror() function returns a string describing the error code passed
     * in the argument errnum, possibly using the LC_MESSAGES part of the
     * current locale to select the appropriate language. This string must not
     * be modified by the application, but may be modified by a subsequent call
     * to perror() or strerror(). No library function will modify this string.
     * 
     * The strerror_r() function is similar to strerror(), but is thread
     * safe. This function is available in two versions: an XSI-compliant
     * version specified in POSIX.1-2001, and a GNU-specific version (available
     * since glibc 2.0). If _XOPEN_SOURCE is defined with the value 600, then
     * the XSI-compliant version is provided, otherwise the GNU-specific version
     * is provided.
     * 
     * The XSI-compliant strerror_r() is preferred for portable applications. It
     * returns the error string in the user-supplied buffer buf of length
     * buflen.
     * 
     * The GNU-specific strerror_r() returns a pointer to a string containing
     * the error message. This may be either a pointer to a string that the
     * function stores in buf, or a pointer to some (immutable) static string
     * (in which case buf is unused). If the function stores a string in buf,
     * then at most buflen bytes are stored (the string may be truncated if
     * buflen is too small) and the string always includes a terminating null
     * byte.
     *
     */
    public static String strerror( int errnum ) {
        return delegate.strerror( errnum );
    }

    public static String strerror() {
        return strerror( errno() );
    }

    /**
     * The <errno.h> header file defines the integer variable errno, which is
     * set by system calls and some library functions in the event of an error
     * to indicate what went wrong. Its value is significant only when the call
     * returned an error (usually -1), and a function that does succeed is
     * allowed to change errno.
     * 
     * Sometimes, when -1 is also a valid successful return value one has to
     * zero errno before the call in order to detect possible errors.
     * 
     * errno is defined by the ISO C standard to be a modifiable lvalue of type
     * int, and must not be explicitly declared; errno may be a macro. errno is
     * thread-local; setting it in one thread does not affect its value in any
     * other thread.
     * 
     * Valid error numbers are all non-zero; errno is never set to zero by any
     * library function. All the error names specified by POSIX.1 must have
     * distinct values, with the exception of EAGAIN and EWOULDBLOCK, which may
     * be the same.
     * 
     * Below is a list of the symbolic error names that are defined on
     * Linux. Some of these are marked POSIX.1, indicating that the name is
     * defined by POSIX.1-2001, or C99, indicating that the name is defined by
     * C99.
     *
     */
    public static int errno() {
        return Native.getLastError();
    }

    interface InterfaceDelegate extends Library {

        int perror( String s );
        String strerror( int errnum );
        
    }

}

