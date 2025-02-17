# Introduction

The intent of this document is to discuss all (or most) of the processes that
Maelstrom creates, as well as their characteristics when it comes to:
responsibilities, progress groups, sessions, namespaces, signal handling, and
termination.

# Worker

There are always at least three processes for the worker. Let's call them `U`,
`V`, and `W`. In addition, when the worker runs jobs, each job will be its own process.

## Process `U` --- Generation 0

This is the worker's grandparent process. It is the process executed by the
shell. It's only job is to clone `V`, the generation 1 process, and then wait
for its completion.

The reason we don't do any work in `U` is that we want to guarantee that we
won't leak running jobs or processes. One easy way to do this is to put all the
job-related subprocesses in their own PID namespace. Then, when PID 1 of that
namespace terminates, all processes in that namespace will also be terminated.
However, we can't move `U` into its own PID namespace. Instead, what we can do
is clone a child process and have it be in its own namespace. That's what
process `V` is.

### Responsibilities

Process `U` clones process `V`, and then waits for process `V` to terminate. It
then tries to mimic process `V`'s termination mode.

### Job Control

Process `U` doesn't do anything special regarding job control. It doesn't
change its process group or its session.

### Namespaces

Process `U` is in the namespaces of its parent. It doesn't `unshare` at all.
Instead, it clones process `V` into its own user and PID namespaces.

### Signal Handling

Process `U` does nothing special regarding signal handling. It's its child's
responsibility to notice when it terminates because of a signal.

### Termination

Process `U` waits for process `V` to terminate, then attempts to terminate
itself in the same manner. Even if it fails to mimic the termination condition
exactly, it will always terminate when `V` terminates. Since it establishes no
signal handlers, it may obviously be killed before `V` terminates as well.

## Process `V` --- Generation 1

This is the worker's parent process.

One might think that process `V` would be the main worker process. Indeed, it
used to be that way, but for the reasons we discuss below, we chose to make the
main worker process a child of process `V`: process `W`.

Process `V` created in its own user and PID namespaces. This means it is PID 1
--- the "init" process --- for the PID namespace. Because of this, some rules
regarding signals and waiting on descendant processes are special for it.

One special aspect of the "init" process for a namespace is that all orphaned
processes in the namespace become children of the "init" process. The "init"
process must wait on these children to prevent the accumulation of zombies.
However, this waiting on zombies interferes with the normal waiting on child
processes that the worker wants to do. Because of this, we adopted the design
where the worker is the second process, PID 2, in the namespace, and thus
doesn't have any zombie-waiting responsibilities. It's `V`'s responsibility to
wait on all orphaned processes that may arise, and to terminate when it sees
process `W` terminate.

### Responsibilities

Process `V` sets up the user namespace, forks process `W`, then waits around
for process `W` to terminate. When process `W` terminates, it attempts to mimic
its termination mode. While it is waiting for process `W` to terminate, process
`V` waits on all descendants, clearning any zombie processes that may arise.

### Job Control

Process `V` doesn't do anything special regarding job control. It doesn't change
its process group or its session.

### Namespaces

Process `V` is in its own user and PID namespaces. It it PID 1 --- the "init"
process --- of its PID namespace. It also maps UID 0 to `U`'s UID, and GID 0 to
`U`'s GID.

### Signal Handling

Since process `V` is PID 1 in a PID namespace, signals (except `SIGKILL` and
`SIGSTOP`) won't be delivered to it unless it has registered handlers for them.
So, process `V` registers handlers for all signals whos default action is to
terminate the process. When it receives one of these signals, it forwards it on
process `W`.

In addition, process `V` registers for a "parent death signal": when process
`U` terminates, process `V` will receive a signal, which it will then forward
to proces `W`.

Process `V` has to be careful when registering signal handlers, as any signal
handler registered before forking will be inherited by process `W`. Process `W`
must then replace these signal handlers.

### Termination

Process `V` waits for process `W` to terminate, then attempts to terminate
itself in the same manner. Even if it fails to mimic the termination condition
exactly, it will always terminate when `W` terminates.

As discussed above, if process `U` terminates, process `V` will receive a
signal which it will then forward to process `W`, which should then terminate
in a reasonable amount of time.

## Process `W` --- Generation 2

This is the main worker process. It connects to the broker, executes jobs it is
sent from the broker, and then receives jobs to execute from the broker, and
then terminates either when the broker disconnects, or it receives a signal.

Each job is run in its own process, which we call process `J`.

### Responsibilities

Being the main worker, this process has a lot of responsibilities. It connects
with the broker. It receives job requests from the broker. It executes
requested jobs in their own processes and namespaces. It sends result back to
the broker.

### Job Control

Process `W` doesn't do anything special regarding job control. It doesn't
change its process group or its session.

### Namespaces

Process `W` doesn't create any namespaces.

### Signal Handling

Process `W` needs to be careful when terminating. Since it exports a FUSE file
system, and since FUSE is kind of buggy, if we just terminate while certain
FUSE operations are in progress, we can leave stuck processes around that are
impossible to get rid of. As a result, process `W` catches signals and
cancels all jobs before terminating.

### Termination

Process `W` terminates in two scenarios. The first is if the broker connection
is closed. The second is if it receives a signal. In both cases, it attempts to
cancel all jobs before terminating.
