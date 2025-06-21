const std = @import("std");

pub fn ObjectPool(comptime T: type) type {
    return struct {
        data: std.ArrayList(T),
        freelist: std.ArrayList(u32),
        count: u32,

        pub fn init(gpa: std.mem.Allocator) @This() {
            return .{
                .data = std.ArrayList(T).init(gpa),
                .freelist = std.ArrayList(u32).init(gpa),
                .count = 0,
            };
        }

        pub fn deinit(self: *@This()) void {
            self.data.deinit();
            self.freelist.deinit();
        }

        pub fn acquire(self: *@This()) !u32 {
            defer self.count +%= 1;
            errdefer self.count -%= 1;
            return self.freelist.pop() orelse {
                @branchHint(.cold);
                const desired_size = @max(8, self.data.capacity * 2);
                // See PendingIndex:
                //   Last value is reserved to mark PendingIndex as
                //   empty, meaning we can't reference that object
                const size = @min(desired_size, std.math.maxInt(u32));

                if (size <= self.data.capacity) {
                    return error.PoolFull;
                }

                try self.freelist.ensureTotalCapacity(size);
                for (self.data.items.len..size) |i|
                    try self.freelist.append(@intCast(i));
                try self.data.resize(size);

                return self.freelist.pop() orelse unreachable;
            };
        }

        pub fn release(self: *@This(), i: u32) void {
            defer self.count -= 1;
            self.freelist.append(i) catch unreachable;
        }

        pub fn get(self: @This(), i: u32) T {
            return self.data.items[@intCast(i)];
        }

        pub fn set(self: *@This(), i: u32, item: T) void {
            self.data.items[@intCast(i)] = item;
        }
    };
}
