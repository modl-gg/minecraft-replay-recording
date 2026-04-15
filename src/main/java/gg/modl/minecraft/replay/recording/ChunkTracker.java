package gg.modl.minecraft.replay.recording;

import com.github.retrooper.packetevents.protocol.player.ClientVersion;
import com.github.retrooper.packetevents.protocol.world.chunk.BaseChunk;
import com.github.retrooper.packetevents.protocol.world.chunk.Column;
import com.github.retrooper.packetevents.protocol.world.chunk.impl.v_1_18.Chunk_v1_18;
import com.github.retrooper.packetevents.protocol.world.states.WrappedBlockState;
import gg.modl.minecraft.replay.util.BlockSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Caches block states from chunk data packets using a shared global cache.
 * One copy of block data per unique chunk (keyed by world + coordinates),
 * with per-player reference sets tracking which chunks each player has loaded.
 * Reference counting ensures chunks are removed when no player needs them.
 * All methods are thread-safe — called from Netty I/O threads and the writer thread.
 */
public class ChunkTracker {

    static final int SECTION_SIZE = 16;
    private static final int SURFACE_DEPTH = 16;
    /** Fixed offset for packBlockPos/unpackBlockPos — works for all MC versions. */
    private static final int PACK_MIN_Y = -64;

    final int minY;
    final int maxY;
    final int sectionsPerChunk;
    private final ServerVersionInfo versionInfo;

    // Global shared chunk data: worldName -> (packedXZ -> RefCountedChunk)
    private final Map<String, ConcurrentHashMap<Long, RefCountedChunk>> globalChunks = new ConcurrentHashMap<>();

    // Per-player loaded chunk keys: playerUuid -> set of WorldChunkRef
    private final Map<UUID, Set<WorldChunkRef>> playerLoadedChunks = new ConcurrentHashMap<>();

    // Per-player current world name
    private final Map<UUID, String> playerWorlds = new ConcurrentHashMap<>();

    private static final String DEFAULT_WORLD = "overworld";

    public ChunkTracker(String mcVersion) {
        this.versionInfo = new ServerVersionInfo(mcVersion);
        this.minY = versionInfo.getMinY();
        this.maxY = versionInfo.getMaxY();
        this.sectionsPerChunk = versionInfo.getSectionsPerChunk();
    }

    /** Backward-compatible no-arg constructor — defaults to 1.18+ world height. */
    public ChunkTracker() {
        this("1.21.4");
    }

    static final class WorldChunkRef {
        final String worldName;
        final long packedXZ;

        WorldChunkRef(String worldName, long packedXZ) {
            this.worldName = worldName;
            this.packedXZ = packedXZ;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof WorldChunkRef)) return false;
            WorldChunkRef that = (WorldChunkRef) o;
            return packedXZ == that.packedXZ && worldName.equals(that.worldName);
        }

        @Override
        public int hashCode() {
            return 31 * worldName.hashCode() + Long.hashCode(packedXZ);
        }
    }

    static class RefCountedChunk {
        volatile ChunkBlockData data;
        final AtomicInteger refCount;

        RefCountedChunk(ChunkBlockData data) {
            this.data = data;
            this.refCount = new AtomicInteger(0);
        }
    }

    public void setPlayerWorld(UUID viewerUuid, String worldName) {
        String oldWorld = playerWorlds.put(viewerUuid, worldName);
        if (oldWorld != null && !oldWorld.equals(worldName)) {
            clearPlayerChunksForWorld(viewerUuid, oldWorld);
        }
    }

    public String getPlayerWorld(UUID viewerUuid) {
        return playerWorlds.getOrDefault(viewerUuid, DEFAULT_WORLD);
    }

    /**
     * Cache chunk data for a specific viewer in the shared global cache.
     * Increments the reference count if this is a new chunk for the player.
     * Returns the ChunkBlockData for optional use by the caller.
     */
    public ChunkBlockData handleChunkData(UUID viewerUuid, int chunkX, int chunkZ, Column column) {
        String worldName = getPlayerWorld(viewerUuid);
        long key = packChunkKey(chunkX, chunkZ);
        ChunkBlockData newData = parseColumn(chunkX, chunkZ, column);

        ConcurrentHashMap<Long, RefCountedChunk> worldMap =
                globalChunks.computeIfAbsent(worldName, k -> new ConcurrentHashMap<>());

        Set<WorldChunkRef> loaded = playerLoadedChunks.computeIfAbsent(viewerUuid, k -> ConcurrentHashMap.newKeySet());
        WorldChunkRef ref = new WorldChunkRef(worldName, key);

        // Atomically update ref count and loaded set inside compute to prevent race with handleChunkUnload
        worldMap.compute(key, (k, existing) -> {
            boolean alreadyLoaded = !loaded.add(ref);
            if (existing == null) {
                RefCountedChunk rc = new RefCountedChunk(newData);
                rc.refCount.set(1);
                return rc;
            }
            existing.data = newData;
            if (!alreadyLoaded) {
                existing.refCount.incrementAndGet();
            }
            return existing;
        });

        return newData;
    }

    public void handleChunkUnload(UUID viewerUuid, int chunkX, int chunkZ) {
        String worldName = getPlayerWorld(viewerUuid);
        long key = packChunkKey(chunkX, chunkZ);
        WorldChunkRef ref = new WorldChunkRef(worldName, key);

        Set<WorldChunkRef> loaded = playerLoadedChunks.get(viewerUuid);
        if (loaded == null || !loaded.remove(ref)) return;

        ConcurrentHashMap<Long, RefCountedChunk> worldMap = globalChunks.get(worldName);
        if (worldMap == null) return;

        worldMap.computeIfPresent(key, (k, rc) -> rc.refCount.decrementAndGet() <= 0 ? null : rc);
    }

    /**
     * Copy the player's loaded chunk keys for off-thread snapshot use.
     * Returns a snapshot of the keys — safe to iterate from any thread after this call.
     */
    public WorldChunkRef[] copyPlayerChunkKeys(UUID viewerUuid) {
        Set<WorldChunkRef> loaded = playerLoadedChunks.get(viewerUuid);
        if (loaded == null) return new WorldChunkRef[0];
        return loaded.toArray(new WorldChunkRef[0]);
    }

    /**
     * Retain a copied set of chunk refs so async snapshotting can outlive player disconnects
     * or world changes that would otherwise drop the shared chunk cache entries.
     */
    public void retainChunkKeys(WorldChunkRef[] chunkKeys) {
        for (WorldChunkRef ref : chunkKeys) {
            ConcurrentHashMap<Long, RefCountedChunk> worldMap = globalChunks.get(ref.worldName);
            if (worldMap == null) continue;
            worldMap.computeIfPresent(ref.packedXZ, (k, rc) -> {
                rc.refCount.incrementAndGet();
                return rc;
            });
        }
    }

    /**
     * Release refs previously retained via {@link #retainChunkKeys(WorldChunkRef[])}.
     */
    public void releaseChunkKeys(WorldChunkRef[] chunkKeys) {
        for (WorldChunkRef ref : chunkKeys) {
            ConcurrentHashMap<Long, RefCountedChunk> worldMap = globalChunks.get(ref.worldName);
            if (worldMap == null) continue;
            worldMap.computeIfPresent(ref.packedXZ, (k, rc) -> rc.refCount.decrementAndGet() <= 0 ? null : rc);
        }
    }

    /**
     * Takes a block snapshot using a pre-copied set of chunk keys.
     * Can be called from any thread (designed for the writer thread).
     * Iterates the provided keys against the global cache with bounds + occlusion culling.
     */
    public List<BlockSnapshot> snapshotFromKeys(WorldChunkRef[] chunkKeys,
                                                 int minX, int maxX, int minY, int maxY,
                                                 int minZ, int maxZ, int radiusBlocks) {
        List<BlockSnapshot> blocks = new ArrayList<>();

        int boundMinX = minX - radiusBlocks;
        int boundMaxX = maxX + radiusBlocks;
        int boundMinZ = minZ - radiusBlocks;
        int boundMaxZ = maxZ + radiusBlocks;
        int boundMinY = minY - SURFACE_DEPTH;
        int boundMaxY = maxY + SURFACE_DEPTH;

        for (WorldChunkRef ref : chunkKeys) {
            ConcurrentHashMap<Long, RefCountedChunk> worldMap = globalChunks.get(ref.worldName);
            if (worldMap == null) continue;
            RefCountedChunk rc = worldMap.get(ref.packedXZ);
            if (rc == null) continue;
            ChunkBlockData data = rc.data;

            int chunkWorldMinX = data.getChunkX() * SECTION_SIZE;
            int chunkWorldMaxX = chunkWorldMinX + 15;
            int chunkWorldMinZ = data.getChunkZ() * SECTION_SIZE;
            int chunkWorldMaxZ = chunkWorldMinZ + 15;
            if (chunkWorldMaxX < boundMinX || chunkWorldMinX > boundMaxX) continue;
            if (chunkWorldMaxZ < boundMinZ || chunkWorldMinZ > boundMaxZ) continue;

            data.forEachBlock((worldX, worldY, worldZ, stateId) -> {
                if (worldX < boundMinX || worldX > boundMaxX) return;
                if (worldZ < boundMinZ || worldZ > boundMaxZ) return;
                if (worldY < boundMinY || worldY > boundMaxY) return;

                if (isEnclosed(ref.worldName, worldX, worldY, worldZ)) return;

                blocks.add(new BlockSnapshot(worldX, (short) worldY, worldZ, stateId));
            });
        }

        return blocks;
    }

    /**
     * Takes a block snapshot for a viewer using their currently loaded chunks.
     * Delegates to snapshotFromKeys after copying the player's chunk key set.
     */
    public List<BlockSnapshot> snapshot(UUID viewerUuid, int minX, int maxX, int minY, int maxY,
                                        int minZ, int maxZ, int radiusBlocks) {
        return snapshotFromKeys(copyPlayerChunkKeys(viewerUuid), minX, maxX, minY, maxY, minZ, maxZ, radiusBlocks);
    }

    public List<BlockSnapshot> snapshot(UUID viewerUuid, int centerX, int centerY, int centerZ, int radiusBlocks) {
        return snapshot(viewerUuid, centerX, centerX, centerY, centerY, centerZ, centerZ, radiusBlocks);
    }

    /**
     * Collect all non-air blocks from a ChunkBlockData without filtering.
     */
    public List<BlockSnapshot> collectAllBlocks(ChunkBlockData chunkData) {
        List<BlockSnapshot> blocks = new ArrayList<>();
        chunkData.collectAllBlocks(blocks);
        return blocks;
    }

    /**
     * Collect blocks adjacent to changed positions that were previously culled as enclosed.
     * Uses the global shared cache for neighbor lookups.
     */
    public List<BlockSnapshot> collectExposedNeighbors(UUID viewerUuid, Set<Long> changedPositions,
                                                        Set<Long> existingPositions) {
        return collectExposedNeighbors(getPlayerWorld(viewerUuid), changedPositions, existingPositions);
    }

    /**
     * World-name variant — safe to call from any thread without per-player state dependency.
     */
    public List<BlockSnapshot> collectExposedNeighbors(String worldName, Set<Long> changedPositions,
                                                        Set<Long> existingPositions) {
        List<BlockSnapshot> extra = new ArrayList<>();
        ConcurrentHashMap<Long, RefCountedChunk> worldMap = globalChunks.get(worldName);
        if (worldMap == null) return extra;

        int[][] offsets = {{1,0,0},{-1,0,0},{0,1,0},{0,-1,0},{0,0,1},{0,0,-1}};

        for (long packed : changedPositions) {
            int[] pos = unpackBlockPos(packed);
            int cx = pos[0], cy = pos[1], cz = pos[2];

            for (int[] off : offsets) {
                int nx = cx + off[0];
                int ny = cy + off[1];
                int nz = cz + off[2];
                if (ny < minY || ny >= maxY) continue;

                long neighborKey = packBlockPos(nx, ny, nz);
                if (existingPositions.contains(neighborKey)) continue;
                if (changedPositions.contains(neighborKey)) continue;

                int chunkX = nx >> 4;
                int chunkZ = nz >> 4;
                RefCountedChunk rc = worldMap.get(packChunkKey(chunkX, chunkZ));
                if (rc == null) continue;
                int stateId = rc.data.getStateId(nx, ny, nz);
                if (stateId <= 0) continue;

                existingPositions.add(neighborKey);
                extra.add(new BlockSnapshot(nx, (short) ny, nz, stateId));
            }
        }

        return extra;
    }

    static long packBlockPos(int x, int y, int z) {
        return ((long)(x & 0x3FFFFFF) << 38) | ((long)((y - PACK_MIN_Y) & 0xFFF) << 26) | (z & 0x3FFFFFFL);
    }

    static int[] unpackBlockPos(long packed) {
        int x = ((int)((packed >> 38) & 0x3FFFFFF) << 6) >> 6;
        int y = (int) ((packed >> 26) & 0xFFF) + PACK_MIN_Y;
        int z = ((int)(packed & 0x3FFFFFF) << 6) >> 6;
        return new int[]{x, y, z};
    }

    /**
     * Look up the cached block state at a world position by world name.
     * Safe to call from any thread — does not depend on per-player state.
     */
    public int getBlockState(String worldName, int x, int y, int z) {
        ConcurrentHashMap<Long, RefCountedChunk> worldMap = globalChunks.get(worldName);
        if (worldMap == null) return 0;
        RefCountedChunk rc = worldMap.get(packChunkKey(x >> 4, z >> 4));
        if (rc == null) return 0;
        return rc.data.getStateId(x, y, z);
    }

    public int getBlockState(UUID viewerUuid, int x, int y, int z) {
        return getBlockState(getPlayerWorld(viewerUuid), x, y, z);
    }

    /**
     * Remove all cached data for a player (on disconnect).
     * Decrements reference counts for all their loaded chunks.
     */
    public void clearPlayer(UUID viewerUuid) {
        Set<WorldChunkRef> loaded = playerLoadedChunks.remove(viewerUuid);
        playerWorlds.remove(viewerUuid);
        if (loaded == null) return;

        for (WorldChunkRef ref : loaded) {
            ConcurrentHashMap<Long, RefCountedChunk> worldMap = globalChunks.get(ref.worldName);
            if (worldMap == null) continue;
            worldMap.computeIfPresent(ref.packedXZ, (k, rc) -> rc.refCount.decrementAndGet() <= 0 ? null : rc);
        }
    }

    public void clearAll() {
        playerLoadedChunks.clear();
        playerWorlds.clear();
        globalChunks.clear();
        opaqueCache.clear();
    }

    private void clearPlayerChunksForWorld(UUID viewerUuid, String worldName) {
        Set<WorldChunkRef> loaded = playerLoadedChunks.get(viewerUuid);
        if (loaded == null) return;

        ConcurrentHashMap<Long, RefCountedChunk> worldMap = globalChunks.get(worldName);
        loaded.removeIf(ref -> {
            if (!ref.worldName.equals(worldName)) return false;
            if (worldMap != null) {
                worldMap.computeIfPresent(ref.packedXZ, (k, rc) -> rc.refCount.decrementAndGet() <= 0 ? null : rc);
            }
            return true;
        });
    }

    // ==================== Opaque / Enclosed Checks ====================

    private final Map<Integer, Boolean> opaqueCache = new ConcurrentHashMap<>();

    private static final Set<String> NON_OPAQUE_KEYWORDS = Collections.unmodifiableSet(new java.util.HashSet<>(java.util.Arrays.asList(
            "glass", "leaves", "water", "lava", "ice", "barrier",
            "slime_block", "honey_block", "tinted_glass",
            "slab", "stairs", "fence", "wall", "gate",
            "grass", "fern", "flower", "bush", "sapling", "seagrass", "kelp",
            "vine", "lily", "azalea", "moss", "spore", "dripleaf",
            "sweet_berry", "cave_vines", "glow_lichen", "hanging_roots",
            "mangrove_roots", "sugar_cane", "cactus", "bamboo", "mushroom",
            "fungus", "wart_block", "nether_sprouts", "roots",
            "wheat", "carrot", "potato", "beetroot", "melon_stem", "pumpkin_stem",
            "cocoa", "torchflower_crop", "pitcher_crop",
            "torch", "lantern", "lever", "button", "pressure_plate",
            "redstone_wire", "repeater", "comparator", "piston_head",
            "door", "trapdoor", "sign",
            "rail", "carpet", "candle", "chain", "bell", "anvil",
            "brewing_stand", "cauldron", "composter", "grindstone",
            "hopper", "lectern", "stonecutter", "enchanting_table",
            "end_rod", "lightning_rod", "pointed_dripstone",
            "scaffolding", "ladder", "cobweb", "snow",
            "head", "skull", "flower_pot", "banner",
            "campfire", "soul_campfire", "bed"
    )));

    private boolean isOpaque(String worldName, int x, int y, int z) {
        if (y < minY || y >= maxY) return false;
        ConcurrentHashMap<Long, RefCountedChunk> worldMap = globalChunks.get(worldName);
        if (worldMap == null) return false;
        RefCountedChunk rc = worldMap.get(packChunkKey(x >> 4, z >> 4));
        if (rc == null) return false;
        int stateId = rc.data.getStateId(x, y, z);
        if (stateId <= 0) return false;
        return opaqueCache.computeIfAbsent(stateId, id -> {
            try {
                WrappedBlockState state = WrappedBlockState.getByGlobalId(versionInfo.toClientVersion(), id);
                String name = state.getType().getName().toString().toLowerCase();
                return NON_OPAQUE_KEYWORDS.stream().noneMatch(name::contains);
            } catch (Exception e) {
                return true;
            }
        });
    }

    private boolean isEnclosed(String worldName, int x, int y, int z) {
        return isOpaque(worldName, x + 1, y, z)
                && isOpaque(worldName, x - 1, y, z)
                && isOpaque(worldName, x, y + 1, z)
                && isOpaque(worldName, x, y - 1, z)
                && isOpaque(worldName, x, y, z + 1)
                && isOpaque(worldName, x, y, z - 1);
    }

    /**
     * Translate a block state ID if needed (e.g., legacy pre-1.13 IDs).
     * Currently a no-op — PacketEvents may already normalize IDs internally.
     * If blocks render as wrong types on pre-1.13, implement translation here.
     */
    int translateStateId(int stateId) {
        return stateId;
    }

    static long packChunkKey(int chunkX, int chunkZ) {
        return ((long) chunkX << 32) | (chunkZ & 0xFFFFFFFFL);
    }

    private ChunkBlockData parseColumn(int chunkX, int chunkZ, Column column) {
        ChunkBlockData data = new ChunkBlockData(chunkX, chunkZ, minY, sectionsPerChunk);
        BaseChunk[] chunks = column.getChunks();
        if (chunks == null) return data;

        for (int sectionIdx = 0; sectionIdx < chunks.length && sectionIdx < sectionsPerChunk; sectionIdx++) {
            BaseChunk section = chunks[sectionIdx];
            if (section == null) continue;

            int sectionY = minY + sectionIdx * SECTION_SIZE;

            for (int localX = 0; localX < SECTION_SIZE; localX++) {
                for (int localY = 0; localY < SECTION_SIZE; localY++) {
                    for (int localZ = 0; localZ < SECTION_SIZE; localZ++) {
                        int stateId;
                        if (section instanceof Chunk_v1_18) {
                            stateId = ((Chunk_v1_18) section).getBlockId(localX, localY, localZ);
                        } else {
                            stateId = section.getBlockId(localX, localY, localZ);
                        }

                        if (stateId != 0) {
                            stateId = translateStateId(stateId);
                            int worldX = chunkX * SECTION_SIZE + localX;
                            int worldY = sectionY + localY;
                            int worldZ = chunkZ * SECTION_SIZE + localZ;
                            data.setBlock(worldX, worldY, worldZ, stateId);
                        }
                    }
                }
            }
        }

        return data;
    }

    // ==================== Compact Block Storage ====================

    @FunctionalInterface
    interface BlockConsumer {
        void accept(int worldX, int worldY, int worldZ, int stateId);
    }

    /**
     * Holds non-air block states for a single chunk column.
     * Uses lazily-allocated short[4096] arrays per 16x16x16 section instead of a HashMap.
     * MC has ~26,800 block states — fits in unsigned short (0-65535).
     * Memory: ~12 active sections x 8 KB = ~96 KB per chunk (vs ~192 KB with ConcurrentHashMap).
     */
    static class ChunkBlockData {
        private final int chunkX;
        private final int chunkZ;
        private final int minY;
        private final int sectionsPerChunk;
        private final short[][] sections;

        ChunkBlockData(int chunkX, int chunkZ, int minY, int sectionsPerChunk) {
            this.chunkX = chunkX;
            this.chunkZ = chunkZ;
            this.minY = minY;
            this.sectionsPerChunk = sectionsPerChunk;
            this.sections = new short[sectionsPerChunk][];
        }

        void setBlock(int worldX, int worldY, int worldZ, int stateId) {
            int sectionIdx = (worldY - minY) >> 4;
            int localX = worldX & 0xF;
            int localZ = worldZ & 0xF;
            int localY = (worldY - minY) & 0xF;

            short[] section = sections[sectionIdx];
            if (section == null) {
                section = new short[4096];
                sections[sectionIdx] = section;
            }
            section[(localY << 8) | (localX << 4) | localZ] = (short) stateId;
        }

        int getStateId(int worldX, int worldY, int worldZ) {
            int sectionIdx = (worldY - minY) >> 4;
            if (sectionIdx < 0 || sectionIdx >= sectionsPerChunk) return 0;
            short[] section = sections[sectionIdx];
            if (section == null) return 0;
            int localX = worldX & 0xF;
            int localZ = worldZ & 0xF;
            int localY = (worldY - minY) & 0xF;
            return section[(localY << 8) | (localX << 4) | localZ] & 0xFFFF;
        }

        void forEachBlock(BlockConsumer consumer) {
            for (int s = 0; s < sectionsPerChunk; s++) {
                short[] section = sections[s];
                if (section == null) continue;
                int sectionBaseY = minY + s * SECTION_SIZE;
                for (int i = 0; i < 4096; i++) {
                    int stateId = section[i] & 0xFFFF;
                    if (stateId == 0) continue;
                    int localY = i >> 8;
                    int localX = (i >> 4) & 0xF;
                    int localZ = i & 0xF;
                    consumer.accept(
                            chunkX * SECTION_SIZE + localX,
                            sectionBaseY + localY,
                            chunkZ * SECTION_SIZE + localZ,
                            stateId
                    );
                }
            }
        }

        void collectAllBlocks(List<BlockSnapshot> out) {
            forEachBlock((worldX, worldY, worldZ, stateId) ->
                    out.add(new BlockSnapshot(worldX, (short) worldY, worldZ, stateId)));
        }

        int getChunkX() { return chunkX; }
        int getChunkZ() { return chunkZ; }
    }
}
