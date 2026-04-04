package gg.modl.minecraft.replay.recording;

import gg.modl.minecraft.replay.ReplayWriter;
import gg.modl.minecraft.replay.api.*;
import gg.modl.minecraft.replay.format.ReplayEvent;
import gg.modl.minecraft.replay.format.ReplayHeader;
import gg.modl.minecraft.replay.format.events.BlockChangeEvent;
import gg.modl.minecraft.replay.format.events.PlayerBlockBreakEvent;
import gg.modl.minecraft.replay.format.events.PlayerBlockPlaceEvent;
import gg.modl.minecraft.replay.format.events.PlayerSpawnEvent;
import gg.modl.minecraft.replay.util.BlockSnapshot;
import gg.modl.minecraft.replay.util.FormatConstants;
import lombok.Getter;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages per-player recording sessions using a rolling buffer.
 * Events are buffered in memory (keeping only the last N seconds).
 * When stop is called, chunk keys are copied on the caller thread (cheap),
 * then the snapshot, overlay, and file write happen on the writer thread.
 * All public methods are thread-safe.
 */
public class RecordingManager {

    private final RecordingConfig config;
    private final File replaysDir;
    private final Logger logger;
    private final Map<UUID, ActiveRecording> recordings = new ConcurrentHashMap<>();
    private final ExecutorService writerExecutor;

    private ChunkTracker chunkTracker;
    private EntityTracker entityTracker;
    private PacketRecorder packetRecorder;

    public RecordingManager(RecordingConfig config, File replaysDir, Logger logger) {
        this.config = config;
        this.replaysDir = replaysDir;
        this.logger = logger;

        int writerThreads = Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors() / 2));
        this.writerExecutor = Executors.newFixedThreadPool(writerThreads, new ThreadFactory() {
            private final java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "ReplayRecording-Writer-" + counter.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
    }

    public void setTrackers(ChunkTracker chunkTracker, EntityTracker entityTracker) {
        this.chunkTracker = chunkTracker;
        this.entityTracker = entityTracker;
    }

    public void setPacketRecorder(PacketRecorder packetRecorder) {
        this.packetRecorder = packetRecorder;
    }

    /**
     * Start recording — just begins buffering, no file ops.
     */
    public ActiveRecording startRecording(UUID targetUuid, String targetName,
                                          int centerX, int centerY, int centerZ) {
        if (recordings.containsKey(targetUuid)) {
            return null;
        }

        ActiveRecording recording = new ActiveRecording(
                UUID.randomUUID(), targetUuid, targetName,
                config.bufferDurationSeconds(), centerX, centerY, centerZ
        );

        recordings.put(targetUuid, recording);
        logger.info("Started recording " + targetName + " (replay: " + recording.getReplayId() + ")");

        return recording;
    }

    /**
     * Backward-compatible overload — ignores snapshot (taken at stop time now).
     */
    public ActiveRecording startRecording(UUID targetUuid, String targetName,
                                          int centerX, int centerY, int centerZ,
                                          List<BlockSnapshot> snapshot) {
        return startRecording(targetUuid, targetName, centerX, centerY, centerZ);
    }

    /**
     * Backward-compatible overload — ignores snapshot and initial events.
     */
    public ActiveRecording startRecording(UUID targetUuid, String targetName,
                                          int centerX, int centerY, int centerZ,
                                          List<BlockSnapshot> snapshot,
                                          List<ReplayEvent> initialEvents) {
        return startRecording(targetUuid, targetName, centerX, centerY, centerZ);
    }

    /**
     * Enqueue an event into the rolling buffer.
     * Called from Netty threads — non-blocking.
     */
    public void enqueueEvent(UUID targetUuid, ReplayEvent event) {
        ActiveRecording recording = recordings.get(targetUuid);
        if (recording != null) {
            recording.getBuffer().pushEvent(event);
        }
    }

    /**
     * Enqueue an event into every active recording's rolling buffer.
     * Called from Bukkit event handlers for global events (block place/break).
     */
    public void enqueueEventToAll(ReplayEvent event) {
        for (ActiveRecording recording : recordings.values()) {
            recording.getBuffer().pushEvent(event);
        }
    }

    /**
     * Create and enqueue a PlayerBlockPlaceEvent to all active recordings.
     */
    public void enqueueBlockPlace(UUID placer, int x, short y, int z, int stateId) {
        if (recordings.isEmpty()) return;
        long now = System.currentTimeMillis();
        for (ActiveRecording recording : recordings.values()) {
            int deltaMs = (int) (now - recording.getStartTime());
            recording.getBuffer().pushEvent(new PlayerBlockPlaceEvent(deltaMs, placer, x, y, z, stateId));
        }
    }

    /**
     * Create and enqueue a PlayerBlockBreakEvent to all active recordings.
     */
    public void enqueueBlockBreak(UUID breaker, int x, short y, int z, int previousStateId) {
        if (recordings.isEmpty()) return;
        long now = System.currentTimeMillis();
        for (ActiveRecording recording : recordings.values()) {
            int deltaMs = (int) (now - recording.getStartTime());
            recording.getBuffer().pushEvent(new PlayerBlockBreakEvent(deltaMs, breaker, x, y, z, previousStateId));
        }
    }

    public boolean isRecording(UUID targetUuid) {
        return recordings.containsKey(targetUuid);
    }

    /**
     * Synchronous stop with explicit position + player spawns (command-driven).
     */
    public ReplayMetadata stopRecording(UUID targetUuid, int centerX, int centerY, int centerZ,
                                         List<ReplayEvent> initialEvents) {
        ActiveRecording recording = recordings.remove(targetUuid);
        if (recording == null) return null;
        recording.updateBounds(centerX, centerY, centerZ);

        String worldName = captureWorldName(recording.getTargetUuid());
        List<ReplayEvent> bufferedEvents = recording.getBuffer().drainPreRoll();
        ChunkTracker.WorldChunkRef[] chunkKeys = copyChunkKeys(recording.getTargetUuid());
        List<BlockSnapshot> snapshot = takeSnapshotFromKeys(chunkKeys, recording);
        applyOverlay(snapshot, recording.getBlockStateOverlay());

        return writeReplayFromDrained(recording, centerX, centerY, centerZ, initialEvents, bufferedEvents, snapshot, worldName);
    }

    /**
     * Synchronous stop with stored center + auto-generated spawns (for stopAll/plugin disable).
     */
    public ReplayMetadata stopRecording(UUID targetUuid) {
        ActiveRecording recording = recordings.remove(targetUuid);
        if (recording == null) return null;

        String worldName = captureWorldName(recording.getTargetUuid());
        List<ReplayEvent> spawns = generatePlayerSpawns(recording.getTargetUuid());
        List<ReplayEvent> bufferedEvents = recording.getBuffer().drainPreRoll();
        ChunkTracker.WorldChunkRef[] chunkKeys = copyChunkKeys(recording.getTargetUuid());
        List<BlockSnapshot> snapshot = takeSnapshotFromKeys(chunkKeys, recording);
        applyOverlay(snapshot, recording.getBlockStateOverlay());

        return writeReplayFromDrained(recording, recording.getCenterX(), recording.getCenterY(), recording.getCenterZ(),
                spawns, bufferedEvents, snapshot, worldName);
    }

    /**
     * Async stop with stored center + auto-generated spawns.
     * Copies chunk keys and world name on caller thread (microseconds), does all heavy work on writer thread.
     */
    public CompletableFuture<ReplayMetadata> stopRecordingAsync(UUID targetUuid) {
        ActiveRecording recording = recordings.remove(targetUuid);
        if (recording == null) return CompletableFuture.completedFuture(null);

        // Cheap work on caller thread — capture state before player potentially disconnects
        String worldName = captureWorldName(recording.getTargetUuid());
        List<ReplayEvent> spawns = generatePlayerSpawns(recording.getTargetUuid());
        List<ReplayEvent> bufferedEvents = recording.getBuffer().drainPreRoll();
        ChunkTracker.WorldChunkRef[] chunkKeys = copyChunkKeys(recording.getTargetUuid());

        // Heavy work on writer thread
        CompletableFuture<ReplayMetadata> future = new CompletableFuture<>();
        writerExecutor.execute(() -> {
            try {
                List<BlockSnapshot> snapshot = takeSnapshotFromKeys(chunkKeys, recording);
                applyOverlay(snapshot, recording.getBlockStateOverlay());
                ReplayMetadata metadata = writeReplayFromDrained(
                        recording, recording.getCenterX(), recording.getCenterY(), recording.getCenterZ(),
                        spawns, bufferedEvents, snapshot, worldName);
                future.complete(metadata);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error stopping recording for " + recording.getTargetName(), e);
                future.complete(null);
            }
        });
        return future;
    }

    /**
     * Async stop with explicit position + player spawns.
     * Copies chunk keys and world name on caller thread (microseconds), does all heavy work on writer thread.
     */
    public CompletableFuture<ReplayMetadata> stopRecordingAsync(UUID targetUuid, int centerX, int centerY, int centerZ,
                                                                 List<ReplayEvent> initialEvents) {
        ActiveRecording recording = recordings.remove(targetUuid);
        if (recording == null) return CompletableFuture.completedFuture(null);

        // Cheap work on caller thread — capture state before player potentially disconnects
        String worldName = captureWorldName(recording.getTargetUuid());
        recording.updateBounds(centerX, centerY, centerZ);
        List<ReplayEvent> bufferedEvents = recording.getBuffer().drainPreRoll();
        ChunkTracker.WorldChunkRef[] chunkKeys = copyChunkKeys(recording.getTargetUuid());

        // Heavy work on writer thread
        CompletableFuture<ReplayMetadata> future = new CompletableFuture<>();
        writerExecutor.execute(() -> {
            try {
                List<BlockSnapshot> snapshot = takeSnapshotFromKeys(chunkKeys, recording);
                applyOverlay(snapshot, recording.getBlockStateOverlay());
                ReplayMetadata metadata = writeReplayFromDrained(
                        recording, centerX, centerY, centerZ, initialEvents, bufferedEvents, snapshot, worldName);
                future.complete(metadata);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error stopping recording for " + recording.getTargetName(), e);
                future.complete(null);
            }
        });
        return future;
    }

    public void stopAll() {
        for (UUID uuid : new ArrayList<>(recordings.keySet())) {
            stopRecording(uuid);
        }
        writerExecutor.shutdown();
        try {
            if (!writerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                writerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            writerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public ActiveRecording getRecording(UUID targetUuid) {
        return recordings.get(targetUuid);
    }

    public Collection<ActiveRecording> getActiveRecordings() {
        return Collections.unmodifiableCollection(recordings.values());
    }

    private ChunkTracker.WorldChunkRef[] copyChunkKeys(UUID viewerUuid) {
        if (chunkTracker == null) return new ChunkTracker.WorldChunkRef[0];
        return chunkTracker.copyPlayerChunkKeys(viewerUuid);
    }

    private String captureWorldName(UUID viewerUuid) {
        if (chunkTracker == null) return "overworld";
        return chunkTracker.getPlayerWorld(viewerUuid);
    }

    private List<BlockSnapshot> takeSnapshotFromKeys(ChunkTracker.WorldChunkRef[] chunkKeys, ActiveRecording recording) {
        if (chunkTracker == null || chunkKeys.length == 0) return new ArrayList<>();
        try {
            return chunkTracker.snapshotFromKeys(chunkKeys,
                    recording.getMinX(), recording.getMaxX(),
                    recording.getMinY(), recording.getMaxY(),
                    recording.getMinZ(), recording.getMaxZ(),
                    config.radiusBlocks());
        } catch (Exception e) {
            logger.warning("Failed to take chunk snapshot: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Apply the eviction overlay to the snapshot.
     * When BlockChangeEvents are evicted from the circular buffer, their final state
     * is recorded in the overlay. This corrects the snapshot to reflect the true world
     * state at the replay's effective t=0 (start of the buffer window).
     */
    /**
     * Apply the eviction overlay to the snapshot in-place.
     * Modifies the list directly — no copy needed since the snapshot is a fresh ArrayList.
     */
    private void applyOverlay(List<BlockSnapshot> snapshot, Map<Long, Integer> overlay) {
        if (overlay.isEmpty()) return;

        Map<Long, Integer> positionIndex = new HashMap<>(snapshot.size());
        for (int i = 0; i < snapshot.size(); i++) {
            BlockSnapshot b = snapshot.get(i);
            positionIndex.put(ChunkTracker.packBlockPos(b.getX(), b.getY(), b.getZ()), i);
        }

        boolean hasRemovals = false;
        for (Map.Entry<Long, Integer> entry : overlay.entrySet()) {
            long packedPos = entry.getKey();
            int stateId = entry.getValue();
            Integer idx = positionIndex.get(packedPos);

            if (stateId == 0) {
                if (idx != null) {
                    snapshot.set(idx, null);
                    hasRemovals = true;
                }
            } else if (idx != null) {
                BlockSnapshot old = snapshot.get(idx);
                snapshot.set(idx, new BlockSnapshot(old.getX(), old.getY(), old.getZ(), stateId));
            } else {
                int[] pos = ChunkTracker.unpackBlockPos(packedPos);
                snapshot.add(new BlockSnapshot(pos[0], (short) pos[1], pos[2], stateId));
            }
        }

        if (hasRemovals) {
            snapshot.removeIf(Objects::isNull);
        }
    }

    private ReplayMetadata writeReplayFromDrained(ActiveRecording recording, int centerX, int centerY, int centerZ,
                                                   List<ReplayEvent> initialEvents, List<ReplayEvent> bufferedEvents,
                                                   List<BlockSnapshot> snapshot, String worldName) {
        if (chunkTracker != null) {
            addBlockChangeContext(worldName, snapshot, bufferedEvents);
        }

        int timeOffset = bufferedEvents.isEmpty() ? 0 : bufferedEvents.get(0).getTimestampDeltaMs();

        FileReplayOutput output = new FileReplayOutput(replaysDir);
        try {
            OutputStream outputStream = output.openStream();
            ReplayWriter writer = new ReplayWriter(outputStream);

            ReplayHeader header = ReplayHeader.builder()
                    .version(FormatConstants.VERSION)
                    .startTime(System.currentTimeMillis())
                    .mcVersion(config.mcVersion())
                    .targetX(centerX)
                    .targetY(centerY)
                    .targetZ(centerZ)
                    .radiusBlocks(config.radiusBlocks())
                    .build();

            writer.writeHeader(header);
            writer.writeSnapshot(snapshot);

            for (ReplayEvent event : initialEvents) {
                writer.writeEvent(event);
            }

            for (ReplayEvent event : bufferedEvents) {
                writer.writeEvent(event, timeOffset);
            }

            writer.flush();
            writer.close();

            long fileSize = 0;
            File file = output.getOutputFile();
            if (file != null) fileSize = file.length();

            long totalEvents = initialEvents.size() + bufferedEvents.size();
            long durationMs = bufferedEvents.isEmpty() ? 0 :
                    bufferedEvents.get(bufferedEvents.size() - 1).getTimestampDeltaMs() - timeOffset;

            ReplayMetadata metadata = ReplayMetadata.builder()
                    .durationMs(durationMs)
                    .eventCount(totalEvents)
                    .fileSizeBytes(fileSize)
                    .outputFile(file)
                    .build();

            output.onComplete(metadata);

            logger.info("Stopped recording " + recording.getTargetName()
                    + " — " + totalEvents + " events, "
                    + (durationMs / 1000) + "s, "
                    + formatFileSize(fileSize));

            return metadata;
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error writing replay for " + recording.getTargetName(), e);
            output.onError(e);
            return null;
        }
    }

    private List<ReplayEvent> generatePlayerSpawns(UUID viewerUuid) {
        if (entityTracker == null) {
            return Collections.emptyList();
        }
        List<EntityTracker.TrackedEntity> visiblePlayers = entityTracker.getVisiblePlayers(viewerUuid);
        List<ReplayEvent> events = new ArrayList<>();
        for (EntityTracker.TrackedEntity player : visiblePlayers) {
            String name = entityTracker.getPlayerName(player.getUuid());
            events.add(new PlayerSpawnEvent(0, player.getUuid(), name,
                    (float) player.getX(), (float) player.getY(), (float) player.getZ(),
                    player.getYaw(), player.getPitch(), new byte[0]));
        }

        if (packetRecorder != null) {
            ReplayEvent equipEvent = packetRecorder.buildInitialEquipmentEvent(viewerUuid);
            if (equipEvent != null) events.add(equipEvent);
            ReplayEvent invEvent = packetRecorder.buildInitialInventoryEvent(viewerUuid);
            if (invEvent != null) events.add(invEvent);
        }

        return events;
    }

    /**
     * Add block change context directly to the snapshot list (in-place).
     * Adds pre-change states for blocks that change during the replay,
     * and neighboring blocks that become exposed when adjacent blocks break.
     */
    private void addBlockChangeContext(String worldName, List<BlockSnapshot> snapshot,
                                       List<ReplayEvent> bufferedEvents) {
        Map<Long, int[]> changedBlocks = new LinkedHashMap<>();
        for (ReplayEvent event : bufferedEvents) {
            if (event instanceof BlockChangeEvent) {
                BlockChangeEvent bce = (BlockChangeEvent) event;
                long key = ChunkTracker.packBlockPos(bce.getX(), bce.getY(), bce.getZ());
                changedBlocks.putIfAbsent(key, new int[]{bce.getX(), bce.getY(), bce.getZ()});
            }
        }
        if (changedBlocks.isEmpty()) return;

        Set<Long> existingPositions = new HashSet<>();
        for (BlockSnapshot block : snapshot) {
            existingPositions.add(ChunkTracker.packBlockPos(block.getX(), block.getY(), block.getZ()));
        }

        for (Map.Entry<Long, int[]> entry : changedBlocks.entrySet()) {
            long key = entry.getKey();
            if (existingPositions.contains(key)) continue;
            int[] pos = entry.getValue();
            int stateId = chunkTracker.getBlockState(worldName, pos[0], pos[1], pos[2]);
            if (stateId > 0) {
                existingPositions.add(key);
                snapshot.add(new BlockSnapshot(pos[0], (short) pos[1], pos[2], stateId));
            }
        }

        snapshot.addAll(chunkTracker.collectExposedNeighbors(worldName, changedBlocks.keySet(), existingPositions));
    }

    private String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
    }

    public void updatePlayerPosition(UUID targetUuid, double x, double y, double z) {
        ActiveRecording recording = recordings.get(targetUuid);
        if (recording != null) {
            recording.updateBounds(x, y, z);
        }
    }

    @Getter
    public static class ActiveRecording {
        private final UUID replayId;
        private final UUID targetUuid;
        private final String targetName;
        private final CircularEventBuffer buffer;
        private final int centerX, centerY, centerZ;
        private final long startTime = System.currentTimeMillis();

        private volatile int minX, maxX, minY, maxY, minZ, maxZ;

        // Block state overlay: evicted BlockChangeEvents apply their new stateId here.
        // Consulted during snapshot to reflect the true t=0 world state.
        private final Map<Long, Integer> blockStateOverlay = new ConcurrentHashMap<>();

        public ActiveRecording(UUID replayId, UUID targetUuid, String targetName,
                               int bufferDurationSeconds, int centerX, int centerY, int centerZ) {
            this.replayId = replayId;
            this.targetUuid = targetUuid;
            this.targetName = targetName;
            this.buffer = new CircularEventBuffer(bufferDurationSeconds, this::onEventEvicted);
            this.centerX = centerX;
            this.centerY = centerY;
            this.centerZ = centerZ;
            this.minX = this.maxX = centerX;
            this.minY = this.maxY = centerY;
            this.minZ = this.maxZ = centerZ;
        }

        private void onEventEvicted(ReplayEvent event) {
            if (event instanceof BlockChangeEvent) {
                BlockChangeEvent bce = (BlockChangeEvent) event;
                long key = ChunkTracker.packBlockPos(bce.getX(), bce.getY(), bce.getZ());
                blockStateOverlay.put(key, bce.getStateId());
            }
        }

        public void updateBounds(double x, double y, double z) {
            int ix = (int) Math.floor(x);
            int iy = (int) Math.floor(y);
            int iz = (int) Math.floor(z);
            if (ix < minX) minX = ix;
            if (ix > maxX) maxX = ix;
            if (iy < minY) minY = iy;
            if (iy > maxY) maxY = iy;
            if (iz < minZ) minZ = iz;
            if (iz > maxZ) maxZ = iz;
        }
    }
}
