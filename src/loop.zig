const std = @import("std");

pub const PendingIndex = packed struct {
    tuple_index: u32,
    pool_index: u32,

    pub fn empty() @This() {
        return PendingIndex.from_u64(std.math.maxInt(u64));
    }

    pub fn is_empty(self: @This()) bool {
        return self.pool_index == std.math.maxInt(u32);
    }

    pub fn to_u64(self: @This()) u64 {
        return @bitCast(self);
    }

    pub fn from_u64(x: u64) @This() {
        return @bitCast(x);
    }
};
