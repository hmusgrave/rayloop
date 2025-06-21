const std = @import("std");
const ObjectPool = @import("object_pool.zig").ObjectPool;
const MappedTuple = @import("mapped_tuple.zig").MappedTuple;
const PendingIndex = @import("loop.zig").PendingIndex;
const default_initialize = @import("mem.zig").default_initialize;
const Ring = @import("io_uring.zig").Ring;

//
// Event Combinators
//
pub fn IoUringSQE(Payload: type, submit: anytype) type {
    return struct {
        pub const State = enum { Submitting, Done };
        pub const Result = CQEResult;

        payload: Payload,

        state: State = .Submitting,
        result: Result = undefined,
        scheduled: bool = false,
        returned_from: PendingIndex = PendingIndex.empty(),
        callback: PendingIndex = PendingIndex.empty(),

        pub fn run(self: *@This(), loop: anytype) !void {
            switch (self.state) {
                .Submitting => {
                    const pwc = try loop.pend_with_callback(CQEResultPendingEvent{}, self);
                    const ring: *std.os.linux.IoUring = loop.context.ring;
                    self.state = .Done;
                    try submit(ring, pwc.pend_loc.to_u64(), self.payload);
                    try pwc.store.store(self.*);
                },
                .Done => {
                    self.result = loop.result_for(CQEResultPendingEvent, self.returned_from);
                },
            }
        }
    };
}

//
// Recurring, bookkeeping events
//
const Timeout = struct {
    pub const State = enum { Waiting, Done };
    pub const Result = void;

    deadline_ns: i128,

    state: State = .Waiting,
    result: Result = {},
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn init(elapsed_ns: i128) @This() {
        return .{ .deadline_ns = std.time.nanoTimestamp() + elapsed_ns };
    }

    pub fn run(self: *@This(), loop: anytype) !void {
        const now = std.time.nanoTimestamp();

        if (now >= self.deadline_ns)
            loop.stop();
    }
};

pub const CQEResult = struct {
    result: i32,
    flags: u32,

    pub fn err(self: @This()) std.os.linux.E {
        const cqe = std.os.linux.io_uring_cqe{ .user_data = undefined, .res = self.result, .flags = self.flags };
        return cqe.err();
    }
};

pub const CQEResultPendingEvent = struct {
    pub const State = enum { Done };
    pub const Result = CQEResult;

    state: State = .Done,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(_: *@This(), _: anytype) !void {}
};

pub const PollCompletions = struct {
    // Schedules any CQE callbacks and submits any pending SQEs
    pub const State = enum { Unpend, Done };
    pub const Result = void;

    state: State = .Unpend,
    result: Result = {},
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(_: *@This(), loop: anytype) !void {
        var _cqes: [32]std.os.linux.io_uring_cqe = undefined;
        const ready = loop.context.ring.cq_ready();
        var completed: usize = 0;
        while (completed < ready) {
            const cqes = _cqes[0..@intCast(try loop.context.ring.copy_cqes(&_cqes, 0))];
            defer completed += cqes.len;

            for (cqes) |*cqe| {
                try loop.unpend_follow_callback(PendingIndex.from_u64(cqe.user_data), CQEResult, .{ .result = cqe.res, .flags = cqe.flags });
            }
        }
        _ = try loop.context.ring.submit();
    }
};

//
// IoUring Events
//
pub const WriteStdout = IoUringSQE([]const u8, struct {
    pub fn _f(ring: *std.os.linux.IoUring, loc: u64, data: []const u8) !void {
        _ = try ring.write(loc, std.os.linux.STDOUT_FILENO, data, 0);
    }
}._f);

pub const ConnectPayload = struct {
    client: i32,
    address: *const std.net.Address,
};

pub const Connect = IoUringSQE(ConnectPayload, struct {
    pub fn _f(ring: *std.os.linux.IoUring, loc: u64, payload: ConnectPayload) !void {
        _ = try ring.connect(loc, payload.client, &payload.address.any, payload.address.getOsSockLen());
    }
}._f);

pub const SocketWritePayload = struct {
    client: i32,
    data: []const u8,
};

pub const SocketWrite = IoUringSQE(SocketWritePayload, struct {
    pub fn _f(ring: *std.os.linux.IoUring, loc: u64, payload: SocketWritePayload) !void {
        _ = try ring.write(loc, payload.client, payload.data, 0);
    }
}._f);

pub const SocketReadPayload = struct {
    client: i32,
    buffer: []u8,
};

pub const SocketRead = IoUringSQE(SocketReadPayload, struct {
    pub fn _f(ring: *std.os.linux.IoUring, loc: u64, payload: SocketReadPayload) !void {
        _ = try ring.recv(loc, payload.client, .{ .buffer = payload.buffer }, 0);
    }
}._f);

//
// Example SSL Request Workload
//
pub const ExampleSSLRequest = struct {
    pub const State = enum { Connect, Write, Read, Done };
    pub const Result = CQEResult;

    client: i32,
    address: *std.net.Address,

    state: State = .Connect,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(self: *@This(), loop: anytype) !void {
        switch (self.state) {
            .Connect => {
                self.state = .Write;
                try loop.schedule_with_callback(Connect{
                    .payload = .{
                        .client = self.client,
                        .address = self.address,
                    },
                }, self);
            },
            .Write => {
                const res = loop.result_for(Connect, self.returned_from);
                switch (res.err()) {
                    .SUCCESS => {
                        std.debug.print("Connect Success\n", .{});
                        try loop.schedule(WriteStdout{ .payload = "Connect Success (io_uring)\n" });
                    },
                    else => |err| std.debug.print("Connect Err: {}\n", .{err}),
                }
                const buffer_send = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";
                self.state = .Read;
                try loop.schedule_with_callback(SocketWrite{
                    .payload = .{
                        .client = self.client,
                        .data = buffer_send[0..],
                    },
                }, self);
            },
            .Read => {
                // TODO: The API for `result_for` is incredibly error-prone
                const res = loop.result_for(SocketWrite, self.returned_from);
                switch (res.err()) {
                    .SUCCESS => std.debug.print("Write Success\n", .{}),
                    else => |err| std.debug.print("Write Err: {}\n", .{err}),
                }
                self.state = .Done;
                try loop.schedule_with_callback(SocketRead{
                    .payload = .{
                        .client = self.client,
                        .buffer = loop.context.recv[0..],
                    },
                }, self);
            },
            .Done => {
                const res = loop.result_for(SocketRead, self.returned_from);
                switch (res.err()) {
                    .SUCCESS => std.debug.print("Read Success ({})\n\n{s}\n", .{ res.result, loop.context.recv[0..@intCast(res.result)] }),
                    else => |err| std.debug.print("Read Err: {}\n", .{err}),
                }
            },
        }
    }
};

//
// Example Request Workload
//
pub const ExampleRequest = struct {
    pub const State = enum { Connect, Write, Read, Done };
    pub const Result = CQEResult;

    client: i32,
    address: *std.net.Address,

    state: State = .Connect,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(self: *@This(), loop: anytype) !void {
        switch (self.state) {
            .Connect => {
                self.state = .Write;
                try loop.schedule_with_callback(Connect{
                    .payload = .{
                        .client = self.client,
                        .address = self.address,
                    },
                }, self);
            },
            .Write => {
                const res = loop.result_for(Connect, self.returned_from);
                switch (res.err()) {
                    .SUCCESS => {
                        std.debug.print("Connect Success\n", .{});
                        try loop.schedule(WriteStdout{ .payload = "Connect Success (io_uring)\n" });
                    },
                    else => |err| std.debug.print("Connect Err: {}\n", .{err}),
                }
                const buffer_send = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";
                self.state = .Read;
                try loop.schedule_with_callback(SocketWrite{
                    .payload = .{
                        .client = self.client,
                        .data = buffer_send[0..],
                    },
                }, self);
            },
            .Read => {
                // TODO: The API for `result_for` is incredibly error-prone
                const res = loop.result_for(SocketWrite, self.returned_from);
                switch (res.err()) {
                    .SUCCESS => std.debug.print("Write Success\n", .{}),
                    else => |err| std.debug.print("Write Err: {}\n", .{err}),
                }
                self.state = .Done;
                try loop.schedule_with_callback(SocketRead{
                    .payload = .{
                        .client = self.client,
                        .buffer = loop.context.recv[0..],
                    },
                }, self);
            },
            .Done => {
                const res = loop.result_for(SocketRead, self.returned_from);
                switch (res.err()) {
                    .SUCCESS => std.debug.print("Read Success ({})\n\n{s}\n", .{ res.result, loop.context.recv[0..@intCast(res.result)] }),
                    else => |err| std.debug.print("Read Err: {}\n", .{err}),
                }
            },
        }
    }
};

//
// Loop
//
pub fn Loop(events: []const type, Context: type) type {
    for (events) |T| {
        if (@sizeOf(T) <= 0)
            @compileError("std bug prevents zero-sized event types: " ++ @typeName(T));
    }

    for (events, 0..) |T, i| {
        for (events[i + 1 ..]) |S| {
            if (S == T)
                @compileError("event " ++ @typeName(T) ++ " appears twice");
        }
    }

    const Fifos = MappedTuple(events, struct {
        fn _f(comptime T: type) type {
            return std.fifo.LinearFifo(T, .Dynamic);
        }
    }._f);

    const Pools = MappedTuple(events, ObjectPool);

    return struct {
        // Work queues, one per Event
        submitted: Fifos,

        // Resizable object pools, one per Event
        pending: Pools,

        // User-configured global context
        context: Context,

        // Cooperative scheduling. Unset this to ask the loop to die.
        running: bool,

        // Zero iff the loop has run to completion. Invariant not always
        // upheld _inside_ loop methods. Some work not explicitly tracked
        // if not necessary for upholding that invariant.
        outstanding_work: usize,

        pub fn init(allocator: std.mem.Allocator, context: Context) !@This() {
            var submitted: Fifos = undefined;
            var pending: Pools = undefined;

            inline for (events, 0..) |_, i| {
                submitted.getByIndex(i).* = .init(allocator);
                pending.getByIndex(i).* = .init(allocator);
            }

            return @This(){
                .submitted = submitted,
                .pending = pending,
                .context = context,
                .running = false,
                .outstanding_work = 0,
            };
        }

        pub fn deinit(self: *@This()) void {
            inline for (events, 0..) |_, i| {
                self.submitted.getByIndex(i).deinit();
                self.pending.getByIndex(i).deinit();
            }
        }

        pub fn result_for(self: *@This(), Event: type, index: PendingIndex) Event.Result {
            return self.pending.getByType(Event).get(index.pool_index).result;
        }

        fn check_pointer_to_struct(T: type) void {
            if (@typeInfo(T) != .pointer or @typeInfo(@typeInfo(T).pointer.child) != .@"struct")
                @compileError(std.fmt.comptimePrint("Received ({s}) instead of pointer to callback event struct", .{@typeName(T)}));
        }

        const Self = @This();
        pub fn Store(Event: type) type {
            return struct {
                loop: *Self,
                loc: PendingIndex,

                pub fn store(self: @This(), event: Event) !void {
                    self.loop.pending.getByType(Event).set(self.loc.pool_index, event);
                }
            };
        }

        pub fn PendWithCallback(Event: type) type {
            return struct {
                pend_loc: PendingIndex,
                store: Store(Event),
            };
        }

        pub fn unpend_follow_callback(self: *@This(), loc: PendingIndex, Result: type, result: Result) !void {
            inline for (events, 0..) |_, i| {
                if (i == loc.tuple_index) {
                    var pool = self.pending.getByIndex(i);
                    var event = pool.get(loc.pool_index);
                    // TODO: This API is incredibly error-prone
                    if (@TypeOf(event).Result != Result)
                        unreachable;
                    event.result = result;
                    pool.set(loc.pool_index, event);
                    inline for (events, 0..) |_, j| {
                        if (j == event.callback.tuple_index) {
                            var callback_pool = self.pending.getByIndex(j);
                            var callback_event = callback_pool.get(event.callback.pool_index);
                            callback_pool.release(event.callback.pool_index);
                            callback_event.returned_from = loc;
                            self.outstanding_work -= 1;
                            try self.schedule(callback_event);
                        }
                    }
                }
            }
        }

        pub fn pend_with_callback(self: *@This(), event: anytype, callback: anytype) !PendWithCallback(@TypeOf(callback.*)) {
            comptime check_pointer_to_struct(@TypeOf(callback));

            const event_tuple_index = comptime @TypeOf(self.pending).getIndex(@TypeOf(event));
            const callback_tuple_index = comptime @TypeOf(self.pending).getIndex(@TypeOf(callback.*));

            var event_pool = self.pending.getByIndex(event_tuple_index);
            var callback_pool = self.pending.getByIndex(callback_tuple_index);

            const event_pool_index = try event_pool.acquire();
            const callback_pool_index = try callback_pool.acquire();

            var event_copy = event;
            event_copy.callback = .{ .tuple_index = callback_tuple_index, .pool_index = callback_pool_index };
            event_pool.set(event_pool_index, event_copy);

            callback.scheduled = true;
            self.outstanding_work += 1;

            return .{
                .pend_loc = .{ .tuple_index = event_tuple_index, .pool_index = event_pool_index },
                .store = .{
                    .loop = self,
                    .loc = event_copy.callback,
                },
            };
        }

        pub fn schedule(self: *@This(), event: anytype) !void {
            try self.submitted.getByType(@TypeOf(event)).writeItem(event);
            self.outstanding_work += 1;
        }

        pub fn schedule_with_callback(self: *@This(), event: anytype, callback: anytype) !void {
            comptime check_pointer_to_struct(@TypeOf(callback));

            const tuple_index = comptime @TypeOf(self.pending).getIndex(@TypeOf(callback.*));
            var pool = self.pending.getByIndex(tuple_index);
            const pool_index = try pool.acquire();

            var event_copy = event;
            event_copy.callback = .{ .tuple_index = tuple_index, .pool_index = pool_index };
            callback.scheduled = true;
            pool.set(pool_index, callback.*);
            try self.schedule(event_copy);
        }

        fn print_state(self: *@This()) void {
            std.debug.print("\nWORK: {}\n", .{self.outstanding_work});
            inline for (events) |T| {
                const pc = self.pending.getByType(T).count;
                const wc = self.submitted.getByType(T).count;
                std.debug.print("{s}: {} {}\n", .{ @typeName(T), pc, wc });
            }
        }

        pub fn run(self: *@This()) !void {
            self.running = true;

            while (self.running and self.outstanding_work > 0) {
                inline for (events, 0..) |_, i| {
                    var submitted_fifo = self.submitted.getByIndex(i);
                    const count = submitted_fifo.count;
                    for (0..count) |_| {
                        var event = submitted_fifo.readItem() orelse unreachable;
                        event.scheduled = false;
                        try event.run(self);
                        self.outstanding_work -= 1; // event itself

                        // If we're returning to this work item from another work item,
                        // we've stored that other work item in `pending` for future use.
                        // That isn't marked as `outstanding_work` because that work always
                        // operates in tandem with the `run` command we just executed, so
                        // we only incremented the thing once. After we've run the command,
                        // the `returned_from` event itself has no further use. We always
                        // execute this, whether or not the event state machine is done,
                        // since it's expected to have fully used or done anything with
                        // the value it read here before moving forward.
                        const returned_from: PendingIndex = event.returned_from;
                        if (!returned_from.is_empty()) {
                            inline for (events, 0..) |_, j| {
                                if (j == returned_from.tuple_index) {
                                    var pool = self.pending.getByIndex(j);
                                    pool.release(returned_from.pool_index);
                                }
                            }
                        }

                        // TODO: Think through this more carefully
                        if (event.scheduled)
                            continue;

                        if (event.state == .Done) {
                            const callback: PendingIndex = event.callback;
                            if (!callback.is_empty()) {
                                // If the event has a callback, let's schedule it for execution.
                                // This _moves_ pending work to scheduled work, so we don't change
                                // the `outstanding_work` counter. We release the callback from
                                // the `pending` pool and store a reference to ourself so that the
                                // callback can examine our return value.
                                inline for (events, 0..) |_, j| {
                                    if (j == callback.tuple_index) {
                                        var callback_pool = self.pending.getByIndex(j);
                                        var callback_event = callback_pool.get(callback.pool_index);
                                        callback_pool.release(callback.pool_index);

                                        var event_pool = self.pending.getByIndex(i);
                                        const pool_index = try event_pool.acquire();
                                        callback_event.returned_from = .{ .tuple_index = i, .pool_index = pool_index };
                                        event_pool.set(pool_index, event);
                                        try self.schedule(callback_event);
                                    }
                                }
                            }
                        } else {
                            try self.schedule(event);
                        }
                    }
                }
            }
        }

        pub fn stop(self: *@This()) void {
            self.running = false;
        }
    };
}

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    var ring: Ring = undefined;
    try ring.setup(32);
    defer ring.deinit();

    var address_list = try std.net.getAddressList(allocator, "example.com", 443);
    defer address_list.deinit();

    if (address_list.addrs.len < 1)
        return error.DnsLookupFailure;

    var address = address_list.addrs[0];
    const client = std.os.linux.socket(address.any.family, std.os.linux.SOCK.STREAM | std.os.linux.SOCK.CLOEXEC, 0);
    defer _ = std.os.linux.close(@intCast(client));

    const Context = struct {
        ring: *std.os.linux.IoUring,
        recv: [1024 * 64]u8,
    };

    var loop = try Loop(&[_]type{ Timeout, WriteStdout, CQEResultPendingEvent, PollCompletions, Connect, SocketWrite, SocketRead, ExampleRequest }, Context).init(std.heap.smp_allocator, .{
        .ring = &ring.ring,
        .recv = undefined,
    });
    defer loop.deinit();

    try loop.schedule(Timeout.init(std.time.ns_per_s));
    try loop.schedule(ExampleSSLRequest{ .client = @intCast(client), .address = &address });
    try loop.schedule(PollCompletions{});
    try loop.run();
}
