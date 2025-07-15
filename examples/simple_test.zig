const std = @import("std");
const wombat = @import("wombat");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.log.info("Testing core Wombat components", .{});

    // Test Arena allocator
    std.log.info("Testing Arena allocator...", .{});
    var arena = try wombat.Arena.init(allocator, 1024 * 1024);
    defer arena.deinit();

    const test_data = try arena.alloc(u8, 100);
    if (test_data.len != 100) {
        std.log.err("Arena allocation failed", .{});
        return;
    }
    std.log.info("Arena allocator: OK", .{});

    // Test SkipList
    std.log.info("Testing SkipList...", .{});
    var skiplist = try wombat.SkipList.init(allocator, 1024 * 1024);
    defer skiplist.deinit();

    const key = "test_key";
    const value = wombat.ValueStruct{
        .value = "test_value",
        .timestamp = 1,
        .meta = 0,
    };

    try skiplist.put(key, value);

    if (skiplist.get(key)) |retrieved| {
        if (!std.mem.eql(u8, retrieved.value, "test_value")) {
            std.log.err("SkipList value mismatch", .{});
            return;
        }
        std.log.info("SkipList: OK", .{});
    } else {
        std.log.err("SkipList get failed", .{});
        return;
    }

    // Test Options
    std.log.info("Testing Options...", .{});
    const options = wombat.Options.default("./test_data")
        .withMemTableSize(1024 * 1024)
        .withSyncWrites(false);

    if (options.mem_table_size != 1024 * 1024) {
        std.log.err("Options configuration failed", .{});
        return;
    }
    std.log.info("Options: OK", .{});

    std.log.info("All core component tests passed!", .{});
}
