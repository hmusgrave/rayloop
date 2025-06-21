const std = @import("std");
const ObjectPool = @import("object_pool.zig").ObjectPool;
const MappedTuple = @import("mapped_tuple.zig").MappedTuple;
const PendingIndex = @import("loop.zig").PendingIndex;
const default_initialize = @import("mem.zig").default_initialize;
const Ring = @import("io_uring.zig").Ring;

const assert = std.debug.assert;

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
                assert(self.request_amt < self.dest.len);
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

                assert(!d.disable_reads);
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
                assert(!self.decoder.disable_reads);
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

pub const TlsWriteState = struct {
    iovecs_buf: [6]std.posix.iovec_const = undefined,
    ciphertext_buf: [std.crypto.tls.max_ciphertext_record_len * 4]u8 = undefined,
};

pub const TlsReadState = struct {
    in_stack_buffer: [vendored_tls.max_ciphertext_len * 4]u8 = undefined,
    vp: vendored_tls.VecPut = undefined,
    cleartext_stack_buffer: [vendored_tls.max_ciphertext_len]u8 = undefined,
    iovecs: [1]std.posix.iovec = undefined,
};

pub const TlsReadvAdvanced = struct {
    pub const State = enum { Init, CQE, Done };
    pub const Result = E!usize;
    pub const E = SQEError || error{ TlsDecodeError, TlsConnectionTruncated, TlsRecordOverflow, TlsBadRecordMac, Overflow, TlsUnexpectedMessage, TlsBadLength, TlsIllegalParameter } || std.crypto.tls.AlertDescription.Error;

    client: i32,
    tls_client: *std.crypto.tls.Client,
    iovecs: []const std.posix.iovec,
    read_state: *TlsReadState,

    vp: vendored_tls.VecPut = undefined,
    first_iov: []u8 = undefined,
    iovec_end: usize = undefined,
    overhead_len: usize = undefined,
    i: usize = 0,
    total_amt: usize = 0,
    actual_read_len: usize = undefined,

    state: State = .Init,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(self: *@This(), loop: anytype) !void {
        _ = loop;
        const c = self.tls_client;
        const iovecs = self.iovecs;

        outer: switch (self.state) {
            .Init => {
                self.read_state.vp = .{ .iovecs = iovecs };

                // Give away the buffered cleartext we have, if any.
                const partial_cleartext = c.partially_read_buffer[c.partial_cleartext_idx..c.partial_ciphertext_idx];
                if (partial_cleartext.len > 0) {
                    const amt: u15 = @intCast(self.read_state.vp.put(partial_cleartext));
                    c.partial_cleartext_idx += amt;

                    if (c.partial_cleartext_idx == c.partial_ciphertext_idx and
                        c.partial_ciphertext_end == c.partial_ciphertext_idx)
                    {
                        // The buffer is now empty.
                        c.partial_cleartext_idx = 0;
                        c.partial_ciphertext_idx = 0;
                        c.partial_ciphertext_end = 0;
                    }

                    if (c.received_close_notify) {
                        c.partial_ciphertext_end = 0;
                        assert(self.read_state.vp.total == amt);
                        self.result = amt;
                        self.state = .Done;
                        return;
                    } else if (amt > 0) {
                        // We don't need more data, so don't call read.
                        assert(self.read_state.vp.total == amt);
                        self.result = amt;
                        self.state = .Done;
                        return;
                    }
                }

                assert(!c.received_close_notify);

                // How many bytes left in the user's buffer.
                const free_size = self.read_state.vp.freeSize();
                // The amount of the user's buffer that we need to repurpose for storing
                // ciphertext. The end of the buffer will be used for such purposes.
                const ciphertext_buf_len = (free_size / 2) -| self.read_state.in_stack_buffer.len;
                // The amount of the user's buffer that will be used to give cleartext. The
                // beginning of the buffer will be used for such purposes.
                const cleartext_buf_len = free_size - ciphertext_buf_len;

                // Recoup `partially_read_buffer` space. This is necessary because it is assumed
                // below that `frag0` is big enough to hold at least one record.
                vendored_tls.limitedOverlapCopy(c.partially_read_buffer[0..c.partial_ciphertext_end], c.partial_ciphertext_idx);
                c.partial_ciphertext_end -= c.partial_ciphertext_idx;
                c.partial_ciphertext_idx = 0;
                c.partial_cleartext_idx = 0;
                self.first_iov = c.partially_read_buffer[c.partial_ciphertext_end..];

                var ask_iovecs_buf: [2]std.posix.iovec = .{
                    .{
                        .base = self.first_iov.ptr,
                        .len = self.first_iov.len,
                    },
                    .{
                        .base = &self.read_state.in_stack_buffer,
                        .len = self.read_state.in_stack_buffer.len,
                    },
                };

                // Cleartext capacity of output buffer, in records. Minimum one full record.
                const buf_cap = @max(cleartext_buf_len / vendored_tls.max_ciphertext_len, 1);
                const wanted_read_len = buf_cap * (vendored_tls.max_ciphertext_len + std.crypto.tls.record_header_len);
                const ask_len = @max(wanted_read_len, self.read_state.cleartext_stack_buffer.len) - c.partial_ciphertext_end;
                const ask_iovecs = vendored_tls.limitVecs(&ask_iovecs_buf, ask_len);

                // TODO: async
                self.actual_read_len = std.os.linux.readv(self.client, ask_iovecs.ptr, ask_iovecs.len);
                self.state = .CQE;
                continue :outer .CQE;
            },
            .CQE => {
                // const res = loop.result_for(Readv, self.returned_from) catch |err| {
                //     self.result = err;
                //     self.state = .Done;
                //     return;
                // };
                // const actual_read_len: usize = @intCast(res.result);

                const actual_read_len: usize = self.actual_read_len;
                if (actual_read_len == 0) {
                    // This is either a truncation attack, a bug in the server, or an
                    // intentional omission of the close_notify message due to truncation
                    // detection handled above the TLS layer.
                    if (c.allow_truncation_attacks) {
                        c.received_close_notify = true;
                    } else {
                        self.result = error.TlsConnectionTruncated;
                        self.state = .Done;
                        return;
                    }
                }

                // There might be more bytes inside `in_stack_buffer` that need to be processed,
                // but at least frag0 will have one complete ciphertext record.
                const frag0_end = @min(c.partially_read_buffer.len, c.partial_ciphertext_end + actual_read_len);
                const frag0 = c.partially_read_buffer[c.partial_ciphertext_idx..frag0_end];
                var frag1 = self.read_state.in_stack_buffer[0..actual_read_len -| self.first_iov.len];
                // We need to decipher frag0 and frag1 but there may be a ciphertext record
                // straddling the boundary. We can handle this with two memcpy() calls to
                // assemble the straddling record in between handling the two sides.
                var frag = frag0;
                var in: usize = 0;
                while (true) {
                    if (in == frag.len) {
                        // Perfect split.
                        if (frag.ptr == frag1.ptr) {
                            c.partial_ciphertext_end = c.partial_ciphertext_idx;
                            self.result = self.read_state.vp.total;
                            self.state = .Done;
                            return;
                        }
                        frag = frag1;
                        in = 0;
                        continue;
                    }

                    if (in + std.crypto.tls.record_header_len > frag.len) {
                        if (frag.ptr == frag1.ptr) {
                            self.result = vendored_tls.finishRead(c, frag, in, self.read_state.vp.total);
                            self.state = .Done;
                            return;
                        }

                        const first = frag[in..];

                        if (frag1.len < std.crypto.tls.record_header_len) {
                            self.result = vendored_tls.finishRead2(c, first, frag1, self.read_state.vp.total);
                            self.state = .Done;
                            return;
                        }

                        // A record straddles the two fragments. Copy into the now-empty first fragment.
                        const record_len_byte_0: u16 = vendored_tls.straddleByte(frag, frag1, in + 3);
                        const record_len_byte_1: u16 = vendored_tls.straddleByte(frag, frag1, in + 4);
                        const record_len = (record_len_byte_0 << 8) | record_len_byte_1;
                        if (record_len > vendored_tls.max_ciphertext_len) {
                            self.result = error.TlsRecordOverflow;
                            self.state = .Done;
                            return;
                        }

                        const full_record_len = record_len + std.crypto.tls.record_header_len;
                        const second_len = full_record_len - first.len;
                        if (frag1.len < second_len) {
                            self.result = vendored_tls.finishRead2(c, first, frag1, self.read_state.vp.total);
                            self.state = .Done;
                            return;
                        }

                        vendored_tls.limitedOverlapCopy(frag, in);
                        @memcpy(frag[first.len..][0..second_len], frag1[0..second_len]);
                        frag = frag[0..full_record_len];
                        frag1 = frag1[second_len..];
                        in = 0;
                        continue;
                    }
                    const ct: std.crypto.tls.ContentType = @enumFromInt(frag[in]);
                    in += 1;
                    const legacy_version = std.mem.readInt(u16, frag[in..][0..2], .big);
                    in += 2;
                    _ = legacy_version;
                    const record_len = std.mem.readInt(u16, frag[in..][0..2], .big);
                    if (record_len > vendored_tls.max_ciphertext_len) {
                        self.result = error.TlsRecordOverflow;
                        self.state = .Done;
                        return;
                    }
                    in += 2;
                    const end = in + record_len;
                    if (end > frag.len) {
                        // We need the record header on the next iteration of the loop.
                        in -= std.crypto.tls.record_header_len;

                        if (frag.ptr == frag1.ptr) {
                            self.result = vendored_tls.finishRead(c, frag, in, self.read_state.vp.total);
                            self.state = .Done;
                            return;
                        }

                        // A record straddles the two fragments. Copy into the now-empty first fragment.
                        const first = frag[in..];
                        const full_record_len = record_len + std.crypto.tls.record_header_len;
                        const second_len = full_record_len - first.len;
                        if (frag1.len < second_len) {
                            self.result = vendored_tls.finishRead2(c, first, frag1, self.read_state.vp.total);
                            self.state = .Done;
                            return;
                        }

                        vendored_tls.limitedOverlapCopy(frag, in);
                        @memcpy(frag[first.len..][0..second_len], frag1[0..second_len]);
                        frag = frag[0..full_record_len];
                        frag1 = frag1[second_len..];
                        in = 0;
                        continue;
                    }
                    const cleartext, const inner_ct: std.crypto.tls.ContentType = cleartext: switch (c.application_cipher) {
                        inline else => |*p| switch (c.tls_version) {
                            .tls_1_3 => {
                                const pv = &p.tls_1_3;
                                const P = @TypeOf(p.*);
                                const ad = frag[in - std.crypto.tls.record_header_len ..][0..std.crypto.tls.record_header_len];
                                const ciphertext_len = record_len - P.AEAD.tag_length;
                                const ciphertext = frag[in..][0..ciphertext_len];
                                in += ciphertext_len;
                                const auth_tag = frag[in..][0..P.AEAD.tag_length].*;
                                const nonce = nonce: {
                                    const V = @Vector(P.AEAD.nonce_length, u8);
                                    const pad = [1]u8{0} ** (P.AEAD.nonce_length - 8);
                                    const operand: V = pad ++ std.mem.toBytes(vendored_tls.big(c.read_seq));
                                    break :nonce @as(V, pv.server_iv) ^ operand;
                                };
                                const out_buf = self.read_state.vp.peek();
                                const cleartext_buf = if (ciphertext.len <= out_buf.len)
                                    out_buf
                                else
                                    &self.read_state.cleartext_stack_buffer;
                                const cleartext = cleartext_buf[0..ciphertext.len];
                                P.AEAD.decrypt(cleartext, ciphertext, auth_tag, ad, nonce, pv.server_key) catch {
                                    self.result = error.TlsBadRecordMac;
                                    self.state = .Done;
                                    return;
                                };
                                const msg = std.mem.trimEnd(u8, cleartext, "\x00");
                                break :cleartext .{ msg[0 .. msg.len - 1], @enumFromInt(msg[msg.len - 1]) };
                            },
                            .tls_1_2 => {
                                const pv = &p.tls_1_2;
                                const P = @TypeOf(p.*);
                                const message_len: u16 = record_len - P.record_iv_length - P.mac_length;
                                const ad = std.mem.toBytes(vendored_tls.big(c.read_seq)) ++
                                    frag[in - std.crypto.tls.record_header_len ..][0 .. 1 + 2] ++
                                    std.mem.toBytes(vendored_tls.big(message_len));
                                const record_iv = frag[in..][0..P.record_iv_length].*;
                                in += P.record_iv_length;
                                const masked_read_seq = c.read_seq &
                                    comptime std.math.shl(u64, std.math.maxInt(u64), 8 * P.record_iv_length);
                                const nonce: [P.AEAD.nonce_length]u8 = nonce: {
                                    const V = @Vector(P.AEAD.nonce_length, u8);
                                    const pad = [1]u8{0} ** (P.AEAD.nonce_length - 8);
                                    const operand: V = pad ++ @as([8]u8, @bitCast(vendored_tls.big(masked_read_seq)));
                                    break :nonce @as(V, pv.server_write_IV ++ record_iv) ^ operand;
                                };
                                const ciphertext = frag[in..][0..message_len];
                                in += message_len;
                                const auth_tag = frag[in..][0..P.mac_length].*;
                                in += P.mac_length;
                                const out_buf = self.read_state.vp.peek();
                                const cleartext_buf = if (message_len <= out_buf.len)
                                    out_buf
                                else
                                    &self.read_state.cleartext_stack_buffer;
                                const cleartext = cleartext_buf[0..ciphertext.len];
                                P.AEAD.decrypt(cleartext, ciphertext, auth_tag, ad, nonce, pv.server_write_key) catch {
                                    self.result = error.TlsBadRecordMac;
                                    self.state = .Done;
                                    return;
                                };
                                break :cleartext .{ cleartext, ct };
                            },
                            else => unreachable,
                        },
                    };
                    c.read_seq = std.math.add(u64, c.read_seq, 1) catch |err| {
                        self.result = err;
                        self.state = .Done;
                        return;
                    };
                    switch (inner_ct) {
                        .alert => {
                            if (cleartext.len != 2) {
                                self.result = error.TlsDecodeError;
                                self.state = .Done;
                                return;
                            }
                            const level: std.crypto.tls.AlertLevel = @enumFromInt(cleartext[0]);
                            const desc: std.crypto.tls.AlertDescription = @enumFromInt(cleartext[1]);
                            if (desc == .close_notify) {
                                c.received_close_notify = true;
                                c.partial_ciphertext_end = c.partial_ciphertext_idx;
                                self.result = self.read_state.vp.total;
                                self.state = .Done;
                                return;
                            }
                            _ = level;

                            desc.toError() catch |err| {
                                self.result = err;
                                self.state = .Done;
                                return;
                            };
                            // TODO: handle server-side closures
                            self.result = error.TlsUnexpectedMessage;
                            self.state = .Done;
                            return;
                        },
                        .handshake => {
                            var ct_i: usize = 0;
                            while (true) {
                                const handshake_type: std.crypto.tls.HandshakeType = @enumFromInt(cleartext[ct_i]);
                                ct_i += 1;
                                const handshake_len = std.mem.readInt(u24, cleartext[ct_i..][0..3], .big);
                                ct_i += 3;
                                const next_handshake_i = ct_i + handshake_len;
                                if (next_handshake_i > cleartext.len) {
                                    self.result = error.TlsBadLength;
                                    self.state = .Done;
                                    return;
                                }
                                const handshake = cleartext[ct_i..next_handshake_i];
                                switch (handshake_type) {
                                    .new_session_ticket => {
                                        // This client implementation ignores new session tickets.
                                    },
                                    .key_update => {
                                        switch (c.application_cipher) {
                                            inline else => |*p| {
                                                const pv = &p.tls_1_3;
                                                const P = @TypeOf(p.*);
                                                const server_secret = vendored_tls.hkdfExpandLabel(P.Hkdf, pv.server_secret, "traffic upd", "", P.Hash.digest_length);
                                                if (c.ssl_key_log) |*key_log| vendored_tls.logSecrets(key_log.file, .{
                                                    .counter = key_log.serverCounter(),
                                                    .client_random = &key_log.client_random,
                                                }, .{
                                                    .SERVER_TRAFFIC_SECRET = &server_secret,
                                                });
                                                pv.server_secret = server_secret;
                                                pv.server_key = vendored_tls.hkdfExpandLabel(P.Hkdf, server_secret, "key", "", P.AEAD.key_length);
                                                pv.server_iv = vendored_tls.hkdfExpandLabel(P.Hkdf, server_secret, "iv", "", P.AEAD.nonce_length);
                                            },
                                        }
                                        c.read_seq = 0;

                                        switch (@as(std.crypto.tls.KeyUpdateRequest, @enumFromInt(handshake[0]))) {
                                            .update_requested => {
                                                switch (c.application_cipher) {
                                                    inline else => |*p| {
                                                        const pv = &p.tls_1_3;
                                                        const P = @TypeOf(p.*);
                                                        const client_secret = vendored_tls.hkdfExpandLabel(P.Hkdf, pv.client_secret, "traffic upd", "", P.Hash.digest_length);
                                                        if (c.ssl_key_log) |*key_log| vendored_tls.logSecrets(key_log.file, .{
                                                            .counter = key_log.clientCounter(),
                                                            .client_random = &key_log.client_random,
                                                        }, .{
                                                            .CLIENT_TRAFFIC_SECRET = &client_secret,
                                                        });
                                                        pv.client_secret = client_secret;
                                                        pv.client_key = vendored_tls.hkdfExpandLabel(P.Hkdf, client_secret, "key", "", P.AEAD.key_length);
                                                        pv.client_iv = vendored_tls.hkdfExpandLabel(P.Hkdf, client_secret, "iv", "", P.AEAD.nonce_length);
                                                    },
                                                }
                                                c.write_seq = 0;
                                            },
                                            .update_not_requested => {},
                                            _ => {
                                                self.result = error.TlsIllegalParameter;
                                                self.state = .Done;
                                                return;
                                            },
                                        }
                                    },
                                    else => {
                                        self.result = error.TlsUnexpectedMessage;
                                        self.state = .Done;
                                        return;
                                    },
                                }
                                ct_i = next_handshake_i;
                                if (ct_i >= cleartext.len) break;
                            }
                        },
                        .application_data => {
                            // Determine whether the output buffer or a stack
                            // buffer was used for storing the cleartext.
                            if (cleartext.ptr == &self.read_state.cleartext_stack_buffer) {
                                // Stack buffer was used, so we must copy to the output buffer.
                                if (c.partial_ciphertext_idx > c.partial_cleartext_idx) {
                                    // We have already run out of room in iovecs. Continue
                                    // appending to `partially_read_buffer`.
                                    @memcpy(
                                        c.partially_read_buffer[c.partial_ciphertext_idx..][0..cleartext.len],
                                        cleartext,
                                    );
                                    c.partial_ciphertext_idx = @intCast(c.partial_ciphertext_idx + cleartext.len);
                                } else {
                                    const amt = self.read_state.vp.put(cleartext);
                                    if (amt < cleartext.len) {
                                        const rest = cleartext[amt..];
                                        c.partial_cleartext_idx = 0;
                                        c.partial_ciphertext_idx = @intCast(rest.len);
                                        @memcpy(c.partially_read_buffer[0..rest.len], rest);
                                    }
                                }
                            } else {
                                // Output buffer was used directly which means no
                                // memory copying needs to occur, and we can move
                                // on to the next ciphertext record.
                                self.read_state.vp.next(cleartext.len);
                            }
                        },
                        else => {
                            self.result = error.TlsUnexpectedMessage;
                            self.state = .Done;
                            return;
                        },
                    }
                    in = end;
                }
            },
            .Done => {},
        }
    }
};

pub const TlsRead = struct {
    pub const State = enum { Init, SQE, CQE, Done };
    pub const Result = E!usize;
    pub const E = SQEError || error{TlsReadAdvanced};

    client: i32,
    tls_client: *std.crypto.tls.Client,
    buffer: []u8,
    read_state: *TlsReadState,

    iovecs: []std.posix.iovec = undefined,
    iovec_end: usize = undefined,
    overhead_len: usize = undefined,
    i: usize = 0,
    total_amt: usize = 0,
    off_i: usize = 0,
    vec_i: usize = 0,
    amt: usize = 0,

    state: State = .Init,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(self: *@This(), loop: anytype) !void {
        const c = self.tls_client;
        const buffer = self.buffer;
        const len = 1;

        outer: switch (self.state) {
            .Init => {
                self.read_state.iovecs = [1]std.posix.iovec{.{ .base = buffer.ptr, .len = buffer.len }};
                self.iovecs = &self.read_state.iovecs;

                if (c.eof()) {
                    self.result = 0;
                    self.state = .Done;
                    return;
                }
                self.state = .SQE;
                continue :outer .SQE;
            },
            .SQE => {
                self.state = .CQE;
                try loop.schedule_with_callback(TlsReadvAdvanced{
                    .client = self.client,
                    .tls_client = self.tls_client,
                    .iovecs = self.iovecs[self.vec_i..],
                    .read_state = self.read_state,
                }, self);
            },
            .CQE => {
                self.amt = loop.result_for(TlsReadvAdvanced, self.returned_from) catch {
                    self.result = error.TlsReadAdvanced;
                    self.state = .Done;
                    return;
                };
                self.off_i += self.amt;
                if (c.eof() or self.off_i >= len) {
                    self.result = self.off_i;
                    self.state = .Done;
                    return;
                }
                while (self.amt >= self.iovecs[self.vec_i].len) {
                    self.amt -= self.iovecs[self.vec_i].len;
                    self.vec_i += 1;
                }
                self.iovecs[self.vec_i].base += self.amt;
                self.iovecs[self.vec_i].len -= self.amt;
                self.state = .SQE;
                continue :outer .SQE;
            },
            .Done => {},
        }
    }
};

// Returns the number of cleartext bytes sent, which may be fewer than `bytes.len`.
pub const TlsWrite = struct {
    pub const State = enum { Init, SQE, CQE, Done };
    pub const Result = SQEError!usize;

    client: i32,
    tls_client: *std.crypto.tls.Client,
    write_state: *TlsWriteState,
    data: []const u8,

    iovec_end: usize = undefined,
    overhead_len: usize = undefined,
    i: usize = 0,
    total_amt: usize = 0,

    state: State = .Init,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(self: *@This(), loop: anytype) !void {
        const c = self.tls_client;
        const bytes = self.data;

        outer: switch (self.state) {
            .Init => {
                self.write_state.* = .{};

                const prepared = vendored_tls.prepareCiphertextRecord(c, &self.write_state.iovecs_buf, &self.write_state.ciphertext_buf, bytes, .application_data);

                self.iovec_end = prepared.iovec_end;
                self.overhead_len = prepared.overhead_len;
                self.state = .SQE;
                continue :outer .SQE;
            },
            .SQE => {
                self.state = .CQE;
                try loop.schedule_with_callback(Writev{
                    .payload = .{
                        .client = self.client,
                        .iovecs = self.write_state.iovecs_buf[self.i..self.iovec_end],
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
                while (amt >= self.write_state.iovecs_buf[self.i].len) {
                    const encrypted_amt = self.write_state.iovecs_buf[self.i].len;
                    self.total_amt += encrypted_amt - self.overhead_len;
                    amt -= encrypted_amt;
                    self.i += 1;
                    // Rely on the property that iovecs delineate records, meaning that
                    // if amt equals zero here, we have fortunately found ourselves
                    // with a short read that aligns at the record boundary.
                    if (self.i >= self.iovec_end) {
                        self.result = self.total_amt;
                        self.state = .Done;
                        return;
                    }
                    // We also cannot return on a vector boundary if the final close_notify is
                    // not sent; otherwise the caller would not know to retry the call.
                    if (amt == 0) {
                        self.result = self.total_amt;
                        self.state = .Done;
                        return;
                    }
                }
                self.write_state.iovecs_buf[self.i].base += amt;
                self.write_state.iovecs_buf[self.i].len -= amt;
                self.state = .SQE;
                continue :outer .SQE;
            },
            .Done => {},
        }
    }
};

pub const ExampleSSLRequest = struct {
    pub const State = enum { Connect, TlsInit, Write, Read, ReadCQE, Done };
    pub const Result = SQEError!void;

    client: i32,
    address: *std.net.Address,
    bundle: *std.crypto.Certificate.Bundle,

    tls_client: *std.crypto.tls.Client = undefined,
    write_state: *TlsWriteState = undefined,
    read_state: *TlsReadState = undefined,
    tls_init: *vendored_tls.TlsInit = undefined,

    state: State = .Connect,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    pub fn run(self: *@This(), loop: anytype) !void {
        switch (self.state) {
            .Connect => {
                self.tls_client = &loop.context.tls_client;
                self.write_state = &loop.context.tls_write_state;
                self.read_state = &loop.context.tls_read_state;
                self.tls_init = &loop.context.tls_init;

                self.state = .TlsInit;
                try loop.schedule_with_callback(Connect{
                    .payload = .{
                        .client = self.client,
                        .address = self.address,
                    },
                }, self);
            },
            .TlsInit => {
                _ = loop.result_for(Connect, self.returned_from) catch |err| {
                    std.debug.print("Connect Err: {}\n", .{err});
                    self.state = .Done;
                    return;
                };
                std.debug.print("Connect Success\n", .{});
                try loop.schedule(WriteStdout{ .payload = "Connect Success (io_uring)\n" });

                self.state = .Write;
                try loop.schedule_with_callback(TlsInit{
                    .client = self.client,
                    .host = "example.com",
                    .tls_init = self.tls_init,
                    .options = .{
                        .host = .{ .explicit = "example.com" },
                        .ca = .{ .bundle = self.bundle.* },
                    },
                }, self);
            },
            .Write => {
                self.tls_client.* = loop.result_for(TlsInit, self.returned_from) catch |err| {
                    std.debug.print("TlsInit Err: {!}\n", .{err});
                    self.state = .Done;
                    return;
                };
                std.debug.print("TLS Initialized\n", .{});

                const buffer_send = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";
                self.state = .Read;
                try loop.schedule_with_callback(TlsWrite{
                    .client = self.client,
                    .tls_client = self.tls_client,
                    .data = buffer_send,
                    .write_state = self.write_state,
                }, self);
            },
            .Read => {
                _ = loop.result_for(TlsWrite, self.returned_from) catch |err| {
                    std.debug.print("Write Err: {!}\n", .{err});
                    self.state = .Done;
                    return;
                };

                self.state = .ReadCQE;
                try loop.schedule_with_callback(TlsRead{
                    .client = self.client,
                    .tls_client = self.tls_client,
                    .buffer = loop.context.recv[0..],
                    .read_state = self.read_state,
                }, self);
            },
            .ReadCQE => {
                const res = loop.result_for(TlsRead, self.returned_from) catch |err| {
                    std.debug.print("Read Err: {!}\n", .{err});
                    self.state = .Done;
                    return;
                };
                const read_size: usize = res;
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
            assert(i == index.tuple_index);
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
                    assert(@TypeOf(event).Result == Result);
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
        tls_write_state: TlsWriteState,
        tls_read_state: TlsReadState,
    };

    var loop = try Loop(&[_]type{ LoopTimeout, WriteStdout, CQEResultPendingEvent, PollCompletions, Connect, Write, Read, ExampleSSLRequest, TlsInit, TlsRead, TlsReadvAdvanced, TlsWrite, WritevAll, Writev, ReadAtLeastOurAmtDecoder, ReadAtLeastDecoder, ReadAtLeast }, Context).init(std.heap.smp_allocator, .{
        .ring = &ring.ring,
        .recv = undefined,
        .tls_client = undefined,
        .tls_init = undefined,
        .tls_write_state = undefined,
        .tls_read_state = undefined,
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
