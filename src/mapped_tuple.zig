const std = @import("std");

pub fn MappedTuple(comptime ts: []const type, comptime map: fn (type) type) type {
    if (ts.len > std.math.maxInt(u32) + 1)
        @compileError(std.fmt.comptimePrint("Unsupported unique type count ({d})", .{ts.len}));

    var fields: [ts.len]std.builtin.Type.StructField = undefined;
    inline for (ts, &fields, 0..) |T, *field, i| {
        const name = std.fmt.comptimePrint("{d}", .{i});
        field.* = .{
            .type = map(T),
            .name = name,
            .alignment = @alignOf(map(T)),
            .is_comptime = false,
            .default_value_ptr = null,
        };
    }

    const TupleType = @Type(.{ .@"struct" = .{
        .fields = &fields,
        .decls = &.{},
        .layout = .auto,
        .is_tuple = true,
        .backing_integer = null,
    } });

    return struct {
        tuple: TupleType,

        pub fn getByIndex(self: *@This(), comptime index: usize) *map(ts[index]) {
            const field_name = std.fmt.comptimePrint("{d}", .{index});
            return &@field(self.tuple, field_name);
        }

        pub fn getByType(self: *@This(), T: type) *map(T) {
            return self.getByIndex(getIndex(T));
        }

        pub fn getIndex(T: type) u32 {
            inline for (ts, 0..) |S, i| {
                if (S == T)
                    return @intCast(i);
            }
            @compileError("Type not found in tuple: " ++ @typeName(T));
        }
    };
}
