const std = @import("std");

pub fn default_initialize(T: type, vals: anytype) T {
    var item: T = undefined;
    inline for (@typeInfo(T).@"struct".fields) |field| {
        comptime var found: bool = false;

        if (field.defaultValue()) |dv| {
            found = true;
            @field(item, field.name) = dv;
        }

        inline for (vals) |val| {
            inline for (@typeInfo(@TypeOf(val)).@"struct".fields) |source_field| {
                if (comptime std.mem.eql(u8, field.name, source_field.name)) {
                    found = true;
                    @field(item, field.name) = @field(val, field.name);
                }
            }
        }

        if (comptime !found)
            @compileError("Incomplete initialization of " ++ @typeName(T));
    }
    return item;
}
