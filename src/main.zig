const std = @import("std");
const ObjectPool = @import("object_pool.zig").ObjectPool;
const MappedTuple = @import("mapped_tuple.zig").MappedTuple;
const PendingIndex = @import("loop.zig").PendingIndex;
const default_initialize = @import("mem.zig").default_initialize;
const Ring = @import("io_uring.zig").Ring;

const vendored_tls = @import("tls.zig");

//
// Errno BS
//
const ErrnoErrorset = blk: {
    const ti = @typeInfo(std.os.linux.E).@"enum";
    var errs: [ti.fields.len]std.builtin.Type.Error = undefined;
    for (0..errs.len) |i|
        errs[i] = .{ .name = ti.fields[i].name };
    break :blk @Type(.{ .error_set = &errs });
};

const errno_lookup = blk: {
    @setEvalBranchQuota(100_000);
    const ti = @typeInfo(std.os.linux.E).@"enum";
    var lookup: [std.math.maxInt(ti.tag_type) + 1]ErrnoErrorset = undefined;
    for (&lookup) |*x|
        x.* = error.SUCCESS;
    for (ti.fields) |field|
        lookup[field.value] = @field(ErrnoErrorset, field.name);
    break :blk lookup;
};

fn to_error_union(e: std.os.linux.E) ErrnoErrorset!void {
    return switch (e) {
        .SUCCESS => {},
        else => |tag| errno_lookup[@intCast(@intFromEnum(tag))],
    };
}

//
// Event Combinators
//
pub const SQEError = ErrnoErrorset || error{SubmissionQueueFull};

pub fn IoUringSQE(Payload: type, submit: anytype) type {
    return struct {
        pub const State = enum { SQE, CQE, Done };
        pub const Result = SQEError!CQEResult;

        payload: Payload,

        state: State = .SQE,
        result: Result = undefined,
        scheduled: bool = false,
        returned_from: PendingIndex = PendingIndex.empty(),
        callback: PendingIndex = PendingIndex.empty(),

        pub fn run(self: *@This(), loop: anytype) !void {
            switch (self.state) {
                .SQE => {
                    const pwc = try loop.pend_with_callback(CQEResultPendingEvent{}, self);
                    const ring: *std.os.linux.IoUring = loop.context.ring;
                    self.state = .CQE;
                    submit(ring, pwc.pend_loc.to_u64(), self.payload) catch |err| {
                        self.result = err;
                        self.scheduled = false;
                        self.state = .Done;
                        return;
                    };
                    pwc.store.store(self.*);
                },
                .CQE => {
                    self.state = .Done;
                    self.result = loop.result_for(CQEResultPendingEvent, self.returned_from);
                },
                .Done => {},
            }
        }
    };
}

//
// Recurring, bookkeeping events
//
const LoopTimeout = struct {
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

    pub fn errno(self: @This()) std.os.linux.E {
        const cqe = std.os.linux.io_uring_cqe{ .user_data = undefined, .res = self.result, .flags = self.flags };
        return cqe.err();
    }

    pub fn err(self: @This()) ErrnoErrorset!@This() {
        try to_error_union(self.errno());
        return self;
    }
};

pub const CQEResultPendingEvent = struct {
    pub const State = enum { Done };
    pub const Result = ErrnoErrorset!CQEResult;

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
                const cqe_result = CQEResult{ .result = cqe.res, .flags = cqe.flags };
                try loop.unpend_follow_callback(PendingIndex.from_u64(cqe.user_data), ErrnoErrorset!CQEResult, cqe_result.err());
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

pub const WritePayload = struct {
    client: i32,
    data: []const u8,
};

pub const Write = IoUringSQE(WritePayload, struct {
    pub fn _f(ring: *std.os.linux.IoUring, loc: u64, payload: WritePayload) !void {
        _ = try ring.write(loc, payload.client, payload.data, 0);
    }
}._f);

pub const WritevPayload = struct {
    client: i32,
    iovecs: []const std.posix.iovec_const,
};

pub const Writev = IoUringSQE(WritevPayload, struct {
    pub fn _f(ring: *std.os.linux.IoUring, loc: u64, payload: WritevPayload) !void {
        _ = try ring.writev(loc, payload.client, payload.iovecs, 0);
    }
}._f);

pub const ReadPayload = struct {
    client: i32,
    buffer: []u8,
};

pub const Read = IoUringSQE(ReadPayload, struct {
    pub fn _f(ring: *std.os.linux.IoUring, loc: u64, payload: ReadPayload) !void {
        _ = try ring.recv(loc, payload.client, .{ .buffer = payload.buffer }, 0);
    }
}._f);

//
// IoUring Composite Events
//
pub const WritevAll = struct {
    pub const State = enum { Init, SQE, CQE, Done };
    pub const Result = SQEError!void;

    client: i32,
    iovecs: []std.posix.iovec_const,
    i: usize = 0,

    state: State = .Init,
    result: Result = {},
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(self: *@This(), loop: anytype) !void {
        outer: switch (self.state) {
            .Init => {
                if (self.iovecs.len == 0) {
                    self.state = .Done;
                    return;
                }
                self.state = .SQE;
                continue :outer .SQE;
            },
            .SQE => {
                self.state = .CQE;
                try loop.schedule_with_callback(Writev{
                    .payload = .{
                        .client = self.client,
                        .iovecs = self.iovecs[self.i..],
                    },
                }, self);
            },
            .CQE => {
                const res = loop.result_for(Writev, self.returned_from) catch |err| {
                    self.result = err;
                    self.state = .Done;
                    return;
                };
                var amt: usize = @intCast(res.result);
                while (amt >= self.iovecs[self.i].len) {
                    amt -= self.iovecs[self.i].len;
                    self.i += 1;
                    if (self.i >= self.iovecs.len) {
                        self.state = .Done;
                        return;
                    }
                }
                self.iovecs[self.i].base += amt;
                self.iovecs[self.i].len -= amt;
                self.state = .SQE;
                continue :outer .SQE;
            },
            .Done => {},
        }
    }
};

//
// Example SSL Request Workload
//
pub const WriteStream = struct {
    pub const WriteError = error{};

    client: i32,

    pub fn writev(self: @This(), iovecs: []const std.posix.iovec_const) WriteError!usize {
        return std.os.linux.writev(self.client, iovecs.ptr, iovecs.len);
    }
};

pub const ReadStream = struct {
    pub const ReadError = error{};

    client: i32,

    pub fn readv(self: @This(), iovecs: []std.posix.iovec) ReadError!usize {
        return std.os.linux.readv(self.client, iovecs.ptr, iovecs.len);
    }
};

pub const ReadAtLeast = struct {
    pub const State = enum { Init, CheckLen, LoopStart, Recv, Done };
    pub const Result = SQEError!usize;

    client: i32,
    dest: []u8,
    request_amt: usize,
    index: usize = 0,

    state: State = .Init,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(self: *@This(), loop: anytype) !void {
        outer: switch (self.state) {
            .Init => {
                std.debug.assert(self.request_amt < self.dest.len);
                self.state = .CheckLen;
                continue :outer .CheckLen;
            },
            .CheckLen => {
                if (self.index < self.request_amt) {
                    self.state = .LoopStart;
                    continue :outer .LoopStart;
                }
                self.result = self.index;
                self.state = .Done;
            },
            .LoopStart => {
                self.state = .Recv;
                try loop.schedule_with_callback(Read{
                    .payload = .{
                        .client = self.client,
                        .buffer = self.dest[self.index..],
                    },
                }, self);
            },
            .Recv => {
                const res: CQEResult = loop.result_for(Read, self.returned_from) catch |err| {
                    self.result = err;
                    self.state = .Done;
                    return;
                };
                const amt: usize = @intCast(res.result);
                if (amt == 0) {
                    self.result = self.index;
                    self.state = .Done;
                    return;
                }
                self.index += amt;
                self.state = .CheckLen;
                continue :outer .CheckLen;
            },
            .Done => {},
        }
    }
};

pub const TLSDecodeError = SQEError || error{ TlsRecordOverflow, TlsConnectionTruncated };

pub const ReadAtLeastDecoder = struct {
    pub const State = enum { Init, Done };
    pub const Result = TLSDecodeError!void;

    client: i32,
    decoder: *std.crypto.tls.Decoder,
    their_amt: usize,
    request_amt: usize = undefined,

    state: State = .Init,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(self: *@This(), loop: anytype) !void {
        switch (self.state) {
            .Init => {
                const d = self.decoder;
                const their_amt = self.their_amt;

                std.debug.assert(!d.disable_reads);
                const existing_amt = d.cap - d.idx;
                d.their_end = d.idx + their_amt;
                if (their_amt <= existing_amt) {
                    self.result = {};
                    self.state = .Done;
                    return;
                }
                self.request_amt = their_amt - existing_amt;
                const dest = d.buf[d.cap..];
                if (self.request_amt > dest.len) {
                    self.result = error.TlsRecordOverflow;
                    self.state = .Done;
                    return;
                }

                self.state = .Done;
                try loop.schedule_with_callback(ReadAtLeast{
                    .client = self.client,
                    .dest = dest,
                    .request_amt = self.request_amt,
                }, self);
            },
            .Done => {
                const d = self.decoder;

                const actual_amt = loop.result_for(ReadAtLeast, self.returned_from) catch |err| {
                    self.result = err;
                    self.state = .Done;
                    return;
                };
                if (actual_amt < self.request_amt) {
                    self.result = error.TlsConnectionTruncated;
                    self.state = .Done;
                    return;
                }
                d.cap += actual_amt;

                self.result = {};
                self.state = .Done;
            },
        }
    }
};

pub const ReadAtLeastOurAmtDecoder = struct {
    pub const State = enum { Init, Done };
    pub const Result = TLSDecodeError!void;

    client: i32,
    decoder: *std.crypto.tls.Decoder,
    our_amt: usize,

    state: State = .Init,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(self: *@This(), loop: anytype) !void {
        switch (self.state) {
            .Init => {
                std.debug.assert(!self.decoder.disable_reads);
                self.state = .Done;
                try loop.schedule_with_callback(ReadAtLeastDecoder{
                    .client = self.client,
                    .decoder = self.decoder,
                    .their_amt = self.our_amt,
                }, self);
            },
            .Done => {
                self.result = loop.result_for(ReadAtLeastDecoder, self.returned_from);
                self.decoder.our_end = self.decoder.idx + self.our_amt;
            },
        }
    }
};

pub const TlsInit = struct {
    pub const State = enum { Init, Recv, Send, Done };
    pub const Result = vendored_tls.InitError(E)!std.crypto.tls.Client;
    pub const E = TLSDecodeError;

    client: i32,
    host: []const u8,
    tls_init: *vendored_tls.TlsInit,
    options: vendored_tls.Options,
    child_state: vendored_tls.TlsInit.RunArg(E) = undefined,
    last_action: vendored_tls.TlsInit.Result = undefined,

    state: State = .Init,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(self: *@This(), loop: anytype) !void {
        outer: switch (self.state) {
            .Init => {
                self.tls_init.* = vendored_tls.TlsInit{};
                self.child_state = .{ .options = self.options };
                self.state = .Send;
                continue :outer .Send;
            },
            .Recv => {
                switch (self.last_action) {
                    .done => unreachable,
                    .action => |act| switch (act) {
                        .writevAll => {
                            loop.result_for(WritevAll, self.returned_from) catch |err| {
                                self.result = err;
                                self.state = .Done;
                                return;
                            };
                            self.child_state = .{ .stream = .{ .writevAll = {} } };
                            self.state = .Send;
                            continue :outer .Send;
                        },
                        .decoder => |dec| switch (dec) {
                            .readAtLeastOurAmt => {
                                const res = loop.result_for(ReadAtLeastOurAmtDecoder, self.returned_from);
                                self.child_state = .{ .stream = .{ .decoder = .{ .readAtLeastOurAmt = res } } };
                                self.state = .Send;
                                continue :outer .Send;
                            },
                            .readAtLeast => {
                                const res = loop.result_for(ReadAtLeastDecoder, self.returned_from);
                                self.child_state = .{ .stream = .{ .decoder = .{ .readAtLeast = res } } };
                                self.state = .Send;
                                continue :outer .Send;
                            },
                        },
                    },
                }
            },
            .Send => {
                self.last_action = self.tls_init.run(E, self.child_state) catch |err| {
                    self.result = err;
                    self.state = .Done;
                    return;
                };
                switch (self.last_action) {
                    .done => |client| {
                        self.result = client;
                        self.state = .Done;
                        return;
                    },
                    .action => |act| switch (act) {
                        .writevAll => |iovecs| {
                            self.state = .Recv;
                            try loop.schedule_with_callback(WritevAll{
                                .client = self.client,
                                .iovecs = iovecs,
                            }, self);
                            return;
                        },
                        .decoder => |dec| switch (dec) {
                            .readAtLeast => |x| {
                                self.state = .Recv;
                                try loop.schedule_with_callback(ReadAtLeastDecoder{
                                    .client = self.client,
                                    .decoder = x.decoder,
                                    .their_amt = x.their_amt,
                                }, self);
                                return;
                            },
                            .readAtLeastOurAmt => |x| {
                                self.state = .Recv;
                                try loop.schedule_with_callback(ReadAtLeastOurAmtDecoder{
                                    .client = self.client,
                                    .decoder = x.decoder,
                                    .our_amt = x.our_amt,
                                }, self);
                                return;
                            },
                        },
                    },
                }
                self.state = .Send;
                continue :outer .Send;
            },
            .Done => {},
        }
    }
};

pub const ExampleSSLRequest = struct {
    pub const State = enum { Connect, Write, PostWrite, Done };
    pub const Result = SQEError!void;

    client: i32,
    address: *std.net.Address,
    bundle: *std.crypto.Certificate.Bundle,
    tls_client: *std.crypto.tls.Client = undefined,

    state: State = .Connect,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(self: *@This(), loop: anytype) !void {
        switch (self.state) {
            .Connect => {
                self.tls_client = &loop.context.tls_client;
                self.state = .Write;
                try loop.schedule_with_callback(Connect{
                    .payload = .{
                        .client = self.client,
                        .address = self.address,
                    },
                }, self);
            },
            .Write => {
                _ = loop.result_for(Connect, self.returned_from) catch |err| {
                    std.debug.print("Connect Err: {}\n", .{err});
                    self.state = .Done;
                    return;
                };
                std.debug.print("Connect Success\n", .{});
                try loop.schedule(WriteStdout{ .payload = "Connect Success (io_uring)\n" });

                self.state = .PostWrite;
                try loop.schedule_with_callback(TlsInit{
                    .client = self.client,
                    .host = "example.com",
                    .tls_init = &loop.context.tls_init,
                    .options = .{
                        .host = .{ .explicit = "example.com" },
                        .ca = .{ .bundle = self.bundle.* },
                    },
                }, self);
            },
            .PostWrite => {
                self.tls_client.* = loop.result_for(TlsInit, self.returned_from) catch |err| {
                    std.debug.print("TlsInit Err: {!}\n", .{err});
                    self.state = .Done;
                    return;
                };
                std.debug.print("TLS Initialized\n", .{});

                const buffer_send = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";
                const write_stream = WriteStream{ .client = self.client };
                _ = self.tls_client.write(write_stream, buffer_send) catch |err| {
                    std.debug.print("Write Err: {!}\n", .{err});
                    self.state = .Done;
                    return;
                };

                const read_stream = ReadStream{ .client = self.client };
                const read_size = self.tls_client.read(read_stream, loop.context.recv[0..]) catch |err| {
                    std.debug.print("Read Err: {!}\n", .{err});
                    self.state = .Done;
                    return;
                };
                const result = loop.context.recv[0..read_size];
                std.debug.print("Read Success: {} {s}\n", .{ result.len, result[0..100] });
                self.state = .Done;
            },
            .Done => {},
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
            const i = comptime @TypeOf(self.pending).getIndex(Event);
            if (i != index.tuple_index) {
                std.debug.print("{} {}\n", .{ i, index });
            }
            std.debug.assert(i == index.tuple_index);
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

                pub fn store(self: @This(), event: Event) void {
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
                    // Assert for logical clarity, if-statement
                    // to allow the following setter to not
                    // be a compile error
                    std.debug.assert(@TypeOf(event).Result == Result);
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
                            try self.schedule(callback_event);
                            self.outstanding_work -= 1;
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
                                        @setEvalBranchQuota(100 * events.len);
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
        tls_client: std.crypto.tls.Client,
        tls_init: vendored_tls.TlsInit,
    };

    var loop = try Loop(&[_]type{ LoopTimeout, WriteStdout, CQEResultPendingEvent, PollCompletions, Connect, Write, Read, ExampleSSLRequest, TlsInit, WritevAll, Writev, ReadAtLeastOurAmtDecoder, ReadAtLeastDecoder, ReadAtLeast }, Context).init(std.heap.smp_allocator, .{
        .ring = &ring.ring,
        .recv = undefined,
        .tls_client = undefined,
        .tls_init = undefined,
    });
    defer loop.deinit();

    var bundle: std.crypto.Certificate.Bundle = .{};
    try bundle.rescan(allocator);
    defer bundle.deinit(allocator);

    try loop.schedule(LoopTimeout.init(std.time.ns_per_s));
    try loop.schedule(ExampleSSLRequest{ .client = @intCast(client), .address = &address, .bundle = &bundle });
    try loop.schedule(PollCompletions{});
    try loop.run();
}
