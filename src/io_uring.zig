const std = @import("std");

pub const Ring = struct {
    params: std.os.linux.io_uring_params = undefined,
    ring: std.os.linux.IoUring = undefined,

    pub fn setup(self: *@This(), entry_count: u16) !void {
        self.params = std.mem.zeroInit(std.os.linux.io_uring_params, .{
            .flags = (
                // Kernel polls submission queue, set in conjunction with sq_thread_idle
                std.os.linux.IORING_SETUP_SQPOLL

                    // Treat all SQEs separately (each posting their own CQE, regardless of
                    // whether others error)
                | std.os.linux.IORING_SETUP_SUBMIT_ALL

                    // Fewer hardware-level interrupts. Not compatible with SQPOLL
                    // | std.os.linux.IORING_SETUP_COOP_TASKRUN

                    // Kernel hint that only one thread submits SQEs. Doesn't necessarily
                    // make sense with SQPOLL, but it can't hurt to duplicate information
                | std.os.linux.IORING_SETUP_SINGLE_ISSUER),
            .sq_thread_idle = 0,
        });
        self.ring = try .init_params(entry_count, &self.params);
    }

    pub fn deinit(self: *@This()) void {
        self.ring.deinit();
    }
};
