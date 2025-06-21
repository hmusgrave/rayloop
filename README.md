Work in progress

This is a basic event loop in Zig. Details like io_uring, kqueue, and other such integrations are in the _userspace_ of the loop. We provide sample io_uring TLS implementations, currently designed as state-machine ports from the standard library.

See [src/main.zig](https://github.com/hmusgrave/rayloop/blob/master/src/main.zig) for more details and examples.
