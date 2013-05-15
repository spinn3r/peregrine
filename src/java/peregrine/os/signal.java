/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine.os;

import java.io.*;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public class signal {

    public static final int SIGHUP          = 1 ;      /* Hangup (POSIX).  */
    public static final int SIGINT          = 2 ;      /* Interrupt (ANSI).  */
    public static final int SIGQUIT         = 3 ;      /* Quit (POSIX).  */
    public static final int SIGILL          = 4 ;      /* Illegal instruction (ANSI).  */
    public static final int SIGTRAP         = 5 ;      /* Trace trap (POSIX).  */
    public static final int SIGABRT         = 6 ;      /* Abort (ANSI).  */
    public static final int SIGIOT          = 6 ;      /* IOT trap (4.2 BSD).  */
    public static final int SIGBUS          = 7 ;      /* BUS error (4.2 BSD).  */
    public static final int SIGFPE          = 8 ;      /* Floating-point exception (ANSI).  */
    public static final int SIGKILL         = 9 ;      /* Kill, unblockable (POSIX).  */
    public static final int SIGUSR1         = 10;      /* User-defined signal 1 (POSIX).  */
    public static final int SIGSEGV         = 11;      /* Segmentation violation (ANSI).  */
    public static final int SIGUSR2         = 12;      /* User-defined signal 2 (POSIX).  */
    public static final int SIGPIPE         = 13;      /* Broken pipe (POSIX).  */
    public static final int SIGALRM         = 14;      /* Alarm clock (POSIX).  */
    public static final int SIGTERM         = 15;      /* Termination (ANSI).  */
    public static final int SIGSTKFLT       = 16;      /* Stack fault.  */
    public static final int SIGCHLD         = 17;      /* Child status has changed (POSIX).  */
    public static final int SIGCONT         = 18;      /* Continue (POSIX).  */
    public static final int SIGSTOP         = 19;      /* Stop, unblockable (POSIX).  */
    public static final int SIGTSTP         = 20;      /* Keyboard stop (POSIX).  */
    public static final int SIGTTIN         = 21;      /* Background read from tty (POSIX).  */
    public static final int SIGTTOU         = 22;      /* Background write to tty (POSIX).  */
    public static final int SIGURG          = 23;      /* Urgent condition on socket (4.2 BSD).  */
    public static final int SIGXCPU         = 24;      /* CPU limit exceeded (4.2 BSD).  */
    public static final int SIGXFSZ         = 25;      /* File size limit exceeded (4.2 BSD).  */
    public static final int SIGVTALRM       = 26;      /* Virtual alarm clock (4.2 BSD).  */
    public static final int SIGPROF         = 27;      /* Profiling alarm clock (4.2 BSD).  */
    public static final int SIGWINCH        = 28;      /* Window size change (4.3 BSD, Sun).  */
    public static final int SIGIO           = 29;      /* I/O now possible (4.2 BSD).  */
    public static final int SIGPWR          = 30;      /* Power failure restart (System V).  */
    public static final int SIGSYS          = 31;      /* Bad system call.  */
    public static final int SIGUNUSED       = 31;

    /**
     * The kill() system call can be used to send any signal to any process
     * group or process.
     * 
     * If pid is positive, then signal sig is sent to pid.
     * 
     * If pid equals 0, then sig is sent to every process in the process group
     * of the current process.
     * 
     * If pid equals -1, then sig is sent to every process for which the calling
     * process has permission to send signals, except for process 1 (init), but
     * see below.
     * 
     * If pid is less than -1, then sig is sent to every process in the process
     * group -pid.
     * 
     * If sig is 0, then no signal is sent, but error checking is still
     * performed.
     * 
     * For a process to have permission to send a signal it must either be
     * privileged (under Linux: have the CAP_KILL capability), or the real or
     * effective user ID of the sending process must equal the real or saved
     * set-user-ID of the target process. In the case of SIGCONT it suffices
     * when the sending and receiving processes belong to the same session.
     * Return Value On success (at least one signal was sent), zero is
     * returned. On error, -1 is returned, and errno is set appropriately.
     * Errors
     * 
     * EINVAL
     * An invalid signal was specified. 
     * 
     * EPERM
     * The process does not have permission to send the signal to any of the target processes. 
     * 
     * ESRCH
     * 
     * The pid or process group does not exist. Note that an existing process
     * might be a zombie, a process which already committed termination, but has
     * not yet been wait()ed for.
     * 
     * Notes
     * 
     * The only signals that can be sent task number one, the init process, are
     * those for which init has explicitly installed signal handlers. This is
     * done to assure the system is not brought down accidentally.
     * 
     * POSIX.1-2001 requires that kill(-1,sig) send sig to all processes that
     * the current process may send signals to, except possibly for some
     * implementation-defined system processes. Linux allows a process to signal
     * itself, but on Linux the call kill(-1,sig) does not signal the current
     * process.
     * 
     * POSIX.1-2001 requires that if a process sends a signal to itself, and the
     * sending thread does not have the signal blocked, and no other thread has
     * it unblocked or is waiting for it in sigwait(), at least one unblocked
     * signal must be delivered to the sending thread before the kill().
     * 
     * Bugs
     * 
     * In 2.6 kernels up to and including 2.6.7, there was a bug that meant that
     * when sending signals to a process group, kill() failed with the error
     * EPERM if the caller did have permission to send the signal to any (rather
     * than all) of the members of the process group. Notwithstanding this error
     * return, the signal was still delivered to all of the processes for which
     * the caller had permission to signal.
     * 
     * Linux History
     * 
     * Across different kernel versions, Linux has enforced different rules for
     * the permissions required for an unprivileged process to send a signal to
     * another process. In kernels 1.0 to 1.2.2, a signal could be sent if the
     * effective user ID of the sender matched that of the receiver, or the real
     * user ID of the sender matched that of the receiver. From kernel 1.2.3
     * until 1.3.77, a signal could be sent if the effective user ID of the
     * sender matched either the real or effective user ID of the receiver. The
     * current rules, which conform to POSIX.1-2001, were adopted in kernel
     * 1.3.78.
     */
    public static int kill( int pid, int sig ) throws PlatformException {

        int result = Delegate.kill( pid, sig );

        if ( result == -1 )
            throw new PlatformException();

        return result;

    }

    static class Delegate {

        public static native int kill( int pid, int sig );
        
        static {
            Native.register( "c" );
        }

    }

}
