const std = @import("std");
const atomic = std.atomic;
const Random = std.Random;
const Arena = @import("../memory/arena.zig").Arena;

pub const ValueStruct = struct {
    value: []const u8,
    timestamp: u64,
    meta: u8,

    pub fn isDeleted(self: *const ValueStruct) bool {
        return (self.meta & 0x01) != 0;
    }

    pub fn setDeleted(self: *ValueStruct) void {
        self.meta |= 0x01;
    }
};

pub const SkipList = struct {
    const MAX_HEIGHT = 20;

    head: *Node,
    height: atomic.Value(u32),
    arena: Arena,
    rng: Random,

    const Self = @This();

    const Node = struct {
        key: []const u8,
        value: ValueStruct,
        height: u16,
        tower: [MAX_HEIGHT]atomic.Value(?*Node),
        mutex: std.Thread.Mutex,

        fn init(key: []const u8, value: ValueStruct, height: u16) Node {
            var node = Node{
                .key = key,
                .value = value,
                .height = height,
                .tower = undefined,
                .mutex = std.Thread.Mutex{},
            };

            for (0..MAX_HEIGHT) |i| {
                node.tower[i] = atomic.Value(?*Node).init(null);
            }

            return node;
        }
    };

    pub fn init(allocator: std.mem.Allocator, arena_size: usize) !Self {
        var arena = try Arena.init(allocator, arena_size);

        const head_key = try arena.allocBytes(0);
        const head_value = ValueStruct{
            .value = &[_]u8{},
            .timestamp = 0,
            .meta = 0,
        };

        const head = try arena.alloc(Node, 1);
        head[0] = Node.init(head_key, head_value, MAX_HEIGHT);

        var prng = std.Random.DefaultPrng.init(0);

        return Self{
            .head = &head[0],
            .height = atomic.Value(u32).init(1),
            .arena = arena,
            .rng = prng.random(),
        };
    }

    pub fn deinit(self: *Self) void {
        self.arena.deinit();
    }

    fn randomHeight(self: *Self) u16 {
        var height: u16 = 1;
        while (height < MAX_HEIGHT and self.rng.boolean()) {
            height += 1;
        }
        return height;
    }

    fn findNode(self: *Self, key: []const u8, prev: *[MAX_HEIGHT]?*Node) ?*Node {
        var current = self.head;
        var level = self.height.load(.acquire);

        while (level > 0) {
            level -= 1;

            while (true) {
                const next = current.tower[level].load(.acquire);
                if (next == null) break;

                const cmp = std.mem.order(u8, next.?.key, key);
                if (cmp == .gt) break;

                current = next.?;
                if (cmp == .eq) {
                    prev[level] = current;
                    return current;
                }
            }

            prev[level] = current;
        }

        return null;
    }

    pub fn put(self: *Self, key: []const u8, value: ValueStruct) !void {
        var prev: [MAX_HEIGHT]?*Node = [_]?*Node{null} ** MAX_HEIGHT;

        if (self.findNode(key, &prev)) |existing| {
            existing.mutex.lock();
            defer existing.mutex.unlock();
            existing.value = value;
            return;
        }

        const node_height = self.randomHeight();

        const key_copy = try self.arena.allocBytes(key.len);
        @memcpy(key_copy, key);

        const new_node = try self.arena.alloc(Node, 1);
        new_node[0] = Node.init(key_copy, value, node_height);

        const current_height = self.height.load(.acquire);
        if (node_height > current_height) {
            for (current_height..node_height) |i| {
                prev[i] = self.head;
            }
            self.height.store(node_height, .release);
        }

        for (0..node_height) |level| {
            const next = if (prev[level]) |p| p.tower[level].load(.acquire) else null;
            new_node[0].tower[level].store(next, .release);

            if (prev[level]) |p| {
                p.tower[level].store(&new_node[0], .release);
            }
        }
    }

    pub fn get(self: *Self, key: []const u8) ?ValueStruct {
        var prev: [MAX_HEIGHT]?*Node = [_]?*Node{null} ** MAX_HEIGHT;

        if (self.findNode(key, &prev)) |node| {
            node.mutex.lock();
            defer node.mutex.unlock();
            return node.value;
        }

        return null;
    }

    pub fn iterator(self: *Self) SkipListIterator {
        return SkipListIterator.init(self);
    }

    pub fn size(self: *Self) usize {
        return self.arena.bytesUsed();
    }
};

pub const SkipListIterator = struct {
    skiplist: *SkipList,
    current: ?*SkipList.Node,

    const Self = @This();

    pub fn init(skiplist: *SkipList) Self {
        return Self{
            .skiplist = skiplist,
            .current = skiplist.head.tower[0].load(.acquire),
        };
    }

    pub fn next(self: *Self) ?struct { key: []const u8, value: ValueStruct } {
        if (self.current) |node| {
            node.mutex.lock();
            defer node.mutex.unlock();
            const result = .{
                .key = node.key,
                .value = node.value,
            };
            self.current = node.tower[0].load(.acquire);
            return result;
        }
        return null;
    }

    pub fn seek(self: *Self, key: []const u8) void {
        var prev: [SkipList.MAX_HEIGHT]?*SkipList.Node = [_]?*SkipList.Node{null} ** SkipList.MAX_HEIGHT;

        if (self.skiplist.findNode(key, &prev)) |node| {
            self.current = node;
        } else {
            self.current = prev[0];
            if (self.current) |c| {
                self.current = c.tower[0].load(.acquire);
            }
        }
    }
};
