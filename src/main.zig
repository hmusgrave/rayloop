const std = @import("std");
const ObjectPool = @import("object_pool.zig").ObjectPool;
const MappedTuple = @import("mapped_tuple.zig").MappedTuple;
const PendingIndex = @import("loop.zig").PendingIndex;
const default_initialize = @import("mem.zig").default_initialize;

const Foo = struct {
    pub const State = enum { Writing1, Writing2, Done };
    pub const Result = void;

    state: State = .Writing1,
    result: Result = {},
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    fn run(self: *@This(), loop: anytype) !void {
        switch (self.state) {
            .Writing1 => {
                std.debug.print("Writing 1\n", .{});
                self.state = .Writing2;
                try loop.schedule_with_callback(Bar{}, self);
            },
            .Writing2 => {
                std.debug.print("Writing 2 {}\n", .{loop.result_for(Bar, self.returned_from)});
                self.state = .Done;
                try loop.schedule(Bar{});
            },
            .Done => {},
        }
    }
};

const Bar = struct {
    pub const State = enum { BarPrint, Done };
    pub const Result = u32;

    state: State = .BarPrint,
    result: Result = undefined,
    scheduled: bool = false,
    returned_from: PendingIndex = PendingIndex.empty(),
    callback: PendingIndex = PendingIndex.empty(),

    fn run(self: *@This(), loop: anytype) !void {
        _ = loop;
        switch (self.state) {
            .BarPrint => {
                self.state = .Done;
                self.result = 42;
                std.debug.print("Bar\n", .{});
            },
            .Done => {},
        }
    }
};

fn Loop(events: []const type, Context: type) type {
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
        submitted: Fifos,
        pending: Pools,
        context: Context,
        running: bool,
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

        // pub fn acquire_and_set_pending(self: *@This(), T: type, data: anytype) !T {
        //     const tuple_index = comptime @TypeOf(self.pending).getIndex(T);
        //     var pool = self.pending.getByIndex(tuple_index);
        //     const pool_index = try pool.acquire();
        //     const item = default_initialize(T, .{
        //         data,
        //         .{
        //             .loc = PendingIndex{
        //                 .tuple_index = tuple_index,
        //                 .pool_index = pool_index,
        //             },
        //             .result = @as(i32, undefined),
        //             .flags = @as(u32, undefined),
        //         },
        //     });
        //     pool.set(pool_index, item);
        //     self.outstanding_work += 1;
        //     return item;
        // }

        pub fn schedule(self: *@This(), event: anytype) !void {
            try self.submitted.getByType(@TypeOf(event)).writeItem(event);
            self.outstanding_work += 1;
        }

        pub fn schedule_with_callback(self: *@This(), event: anytype, callback: anytype) !void {
            if (@typeInfo(@TypeOf(callback)) != .pointer or @typeInfo(@typeInfo(@TypeOf(callback)).pointer.child) != .@"struct")
                @compileError(std.fmt.comptimePrint("Received ({s}) instead of pointer to callback event struct", .{@typeName(@TypeOf(callback))}));

            const tuple_index = comptime @TypeOf(self.pending).getIndex(@TypeOf(callback.*));
            var pool = self.pending.getByIndex(tuple_index);
            const pool_index = try pool.acquire();

            var event_copy = event;
            event_copy.callback = .{ .tuple_index = tuple_index, .pool_index = pool_index };
            callback.scheduled = true;
            pool.set(pool_index, callback.*);
            self.outstanding_work += 1; // callback
            try self.schedule(event_copy);
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
                                        self.outstanding_work -= 1;
                                        try self.schedule(callback_event);
                                    }
                                }
                            }
                        } else if (!event.scheduled) {
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
    var loop = try Loop(&[_]type{ Foo, Bar }, void).init(std.heap.smp_allocator, {});
    defer loop.deinit();
    try loop.schedule(Foo{});
    try loop.run();
}
