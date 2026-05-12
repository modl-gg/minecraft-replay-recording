package gg.modl.minecraft.replay.recording;

import com.github.retrooper.packetevents.PacketEvents;
import com.github.retrooper.packetevents.event.PacketListenerAbstract;
import com.github.retrooper.packetevents.event.PacketListenerPriority;
import com.github.retrooper.packetevents.event.PacketReceiveEvent;
import com.github.retrooper.packetevents.event.PacketSendEvent;
import com.github.retrooper.packetevents.protocol.entity.data.EntityData;
import com.github.retrooper.packetevents.protocol.entity.type.EntityType;
import com.github.retrooper.packetevents.protocol.entity.type.EntityTypes;
import com.github.retrooper.packetevents.protocol.item.ItemStack;
import com.github.retrooper.packetevents.protocol.packettype.PacketType;
import com.github.retrooper.packetevents.protocol.player.ClientVersion;
import com.github.retrooper.packetevents.protocol.player.Equipment;
import com.github.retrooper.packetevents.protocol.player.EquipmentSlot;
import com.github.retrooper.packetevents.protocol.player.InteractionHand;
import com.github.retrooper.packetevents.protocol.player.TextureProperty;
import com.github.retrooper.packetevents.protocol.player.User;
import com.github.retrooper.packetevents.protocol.player.UserProfile;
import com.github.retrooper.packetevents.protocol.world.chunk.BaseChunk;
import com.github.retrooper.packetevents.protocol.world.chunk.Column;
import com.github.retrooper.packetevents.wrapper.play.client.WrapperPlayClientAnimation;
import com.github.retrooper.packetevents.wrapper.play.client.WrapperPlayClientHeldItemChange;
import com.github.retrooper.packetevents.wrapper.play.client.WrapperPlayClientPlayerFlying;
import com.github.retrooper.packetevents.wrapper.play.client.WrapperPlayClientPlayerPosition;
import com.github.retrooper.packetevents.wrapper.play.client.WrapperPlayClientPlayerPositionAndRotation;
import com.github.retrooper.packetevents.wrapper.play.client.WrapperPlayClientPlayerRotation;
import com.github.retrooper.packetevents.protocol.item.enchantment.Enchantment;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerBlockChange;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerChunkData;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerChunkDataBulk;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerCollectItem;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerDestroyEntities;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerEntityAnimation;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerEntityEffect;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerEntityEquipment;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerEntityMetadata;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerEntityRelativeMove;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerEntityRelativeMoveAndRotation;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerEntityTeleport;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerHeldItemChange;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerHurtAnimation;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerJoinGame;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerMultiBlockChange;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerPlayerInfoUpdate;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerRespawn;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerSetSlot;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerSpawnEntity;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerSystemChatMessage;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerUnloadChunk;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerUpdateHealth;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerWindowItems;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import gg.modl.minecraft.replay.format.ReplayEvent;
import gg.modl.minecraft.replay.format.events.BlockChangeEvent;
import gg.modl.minecraft.replay.format.events.ChatEvent;
import gg.modl.minecraft.replay.format.events.EntityMoveEvent;
import gg.modl.minecraft.replay.format.events.EntityRemoveEvent;
import gg.modl.minecraft.replay.format.events.EntitySpawnEvent;
import gg.modl.minecraft.replay.format.events.PlayerAnimEvent;
import gg.modl.minecraft.replay.format.events.PlayerEffectsEvent;
import gg.modl.minecraft.replay.format.events.PlayerEquipmentFullEvent;
import gg.modl.minecraft.replay.format.events.PlayerHealthEvent;
import gg.modl.minecraft.replay.format.events.PlayerInventoryEvent;
import gg.modl.minecraft.replay.format.events.PlayerMoveEvent;
import gg.modl.minecraft.replay.format.events.PlayerRemoveEvent;
import gg.modl.minecraft.replay.format.events.PlayerSkinEvent;
import gg.modl.minecraft.replay.format.events.PlayerSpawnEvent;


import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Intercepts outbound packets via PacketEvents on Netty I/O threads.
 * Translates packets to ReplayEvent objects and enqueues them for recording.
 * Never touches the main server thread.
 *
 * Decoupled from any specific plugin — uses RecordingManager and RecordingConfig.
 */
public class PacketRecorder extends PacketListenerAbstract {

    private final RecordingManager recordingManager;
    private final RecordingConfig config;
    private final Logger logger;
    private final EntityTracker entityTracker;
    private final ChunkTracker chunkTracker;
    private final ClientVersion configuredClientVersion;
    private final ExecutorService skinDownloadExecutor;

    // Per-player last move timestamp for throttling (entity moves)
    private final Map<UUID, Map<Integer, Long>> lastMoveTimestamps = new ConcurrentHashMap<>();

    // Per-player self-move throttle (client position packets)
    private final Map<UUID, Long> lastSelfMoveTimestamps = new ConcurrentHashMap<>();

    // Per-player sneak state tracking
    private final Map<UUID, Map<Integer, Boolean>> sneakStates = new ConcurrentHashMap<>();

    // Per-player last health value for self-damage detection
    private final Map<UUID, Float> lastHealthValues = new ConcurrentHashMap<>();

    // Per-player held slot index (0-8), default 0
    private final Map<UUID, Integer> heldSlots = new ConcurrentHashMap<>();

    // Per-player inventory cache: slot index -> item name
    private final Map<UUID, Map<Integer, String>> inventoryCache = new ConcurrentHashMap<>();

    // Per-player inventory count cache: slot index -> stack count
    private final Map<UUID, Map<Integer, Integer>> inventoryCountCache = new ConcurrentHashMap<>();

    // Per-viewer set of entity UUIDs whose skins have been recorded
    private final Map<UUID, Set<UUID>> skinRecorded = new ConcurrentHashMap<>();

    // Per-viewer equipment cache for other players: viewerUuid -> (playerUuid -> (slotId -> itemName))
    private final Map<UUID, Map<UUID, Map<Integer, String>>> otherPlayerEquipment = new ConcurrentHashMap<>();

    // Per-player inventory enchantment cache: slot index -> list of enchantments
    private final Map<UUID, Map<Integer, List<PlayerInventoryEvent.EnchantEntry>>> inventoryEnchantCache = new ConcurrentHashMap<>();

    private static final int SKIN_TIMEOUT_MS = 5000;

    public PacketRecorder(RecordingManager recordingManager, RecordingConfig config, Logger logger) {
        this(recordingManager, config, logger, createSkinDownloadExecutor());
    }

    PacketRecorder(RecordingManager recordingManager, RecordingConfig config, Logger logger,
                   ExecutorService skinDownloadExecutor) {
        super(PacketListenerPriority.MONITOR);
        this.recordingManager = recordingManager;
        this.config = config;
        this.logger = logger;
        this.entityTracker = new EntityTracker();
        this.chunkTracker = new ChunkTracker(config.mcVersion());
        this.configuredClientVersion = new ServerVersionInfo(config.mcVersion()).toClientVersion();
        this.skinDownloadExecutor = skinDownloadExecutor;
        recordingManager.setTrackers(chunkTracker, entityTracker);
    }

    public EntityTracker getEntityTracker() {
        return entityTracker;
    }

    public ChunkTracker getChunkTracker() {
        return chunkTracker;
    }

    /**
     * Seed the target player into their own EntityTracker so they appear in the replay.
     * Called when recording starts — the player never receives SPAWN_ENTITY for themselves.
     * Uses the real entityId so server packets about self (ENTITY_METADATA, ENTITY_EFFECT) work.
     */
    public void trackSelf(UUID playerUuid, String playerName, int entityId,
                          double x, double y, double z, float yaw, float pitch) {
        entityTracker.trackPlayer(playerUuid, entityId, playerUuid, playerName, x, y, z, yaw, pitch);
        entityTracker.cachePlayerName(playerUuid, playerName);
    }

    public void register() {
        PacketEvents.getAPI().getEventManager().registerListener(this);
    }

    public void unregister() {
        try {
            PacketEvents.getAPI().getEventManager().unregisterListener(this);
        } finally {
            shutdownSkinDownloadExecutor();
            entityTracker.clear();
            chunkTracker.clearAll();
            lastMoveTimestamps.clear();
            sneakStates.clear();
            lastHealthValues.clear();
            heldSlots.clear();
            inventoryCache.clear();
            inventoryCountCache.clear();
            inventoryEnchantCache.clear();
            skinRecorded.clear();
            otherPlayerEquipment.clear();
        }
    }

    void shutdownSkinDownloadExecutor() {
        skinDownloadExecutor.shutdown();
        try {
            if (!skinDownloadExecutor.awaitTermination(SKIN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                skinDownloadExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            skinDownloadExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static ExecutorService createSkinDownloadExecutor() {
        AtomicInteger counter = new AtomicInteger(1);
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "ReplaySkinDownload-" + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
        return Executors.newFixedThreadPool(4, threadFactory);
    }

    @Override
    public void onPacketSend(PacketSendEvent event) {
        UUID viewerUuid = resolvePacketPlayerUuid(event.getUser(), event.getPlayer());
        if (viewerUuid == null) return;

        Object type = event.getPacketType();

        try {
            ReadOnlyPacketEventScope.run(event, () -> {
                if (handleAlwaysTracked(event, type, viewerUuid)) return;

                // Everything below only runs when actively recording
                if (!recordingManager.isRecording(viewerUuid)) return;

                handleRecordingPacket(event, type, viewerUuid);
            });
        } catch (Exception e) {
            logger.warning("Error processing packet " + type + ": " + e.getMessage());
        }
    }

    /**
     * Intercepts client-to-server packets to track the target player's own state.
     * Handles position (self never receives SPAWN_ENTITY) and swing animation
     * (server doesn't send ENTITY_ANIMATION to self).
     */
    @Override
    public void onPacketReceive(PacketReceiveEvent event) {
        UUID playerUuid = resolvePacketPlayerUuid(event.getUser(), event.getPlayer());
        if (playerUuid == null) return;

        Object type = event.getPacketType();

        try {
            ReadOnlyPacketEventScope.run(event, () -> {
                if (type == PacketType.Play.Client.HELD_ITEM_CHANGE) {
                    WrapperPlayClientHeldItemChange wrapper = new WrapperPlayClientHeldItemChange(event);
                    int newSlot = wrapper.getSlot();
                    int oldSlot = heldSlots.getOrDefault(playerUuid, 0);
                    heldSlots.put(playerUuid, newSlot);
                    if (oldSlot != newSlot && recordingManager.isRecording(playerUuid)) {
                        emitSelfEquipment(playerUuid);
                    }
                    return;
                }

                // Extract position/rotation from C2S packets
                double px = Double.NaN, py = Double.NaN, pz = Double.NaN;
                float pYaw = Float.NaN, pPitch = Float.NaN;

                if (type == PacketType.Play.Client.PLAYER_POSITION) {
                    WrapperPlayClientPlayerPosition wrapper = new WrapperPlayClientPlayerPosition(event);
                    px = wrapper.getLocation().getX(); py = wrapper.getLocation().getY(); pz = wrapper.getLocation().getZ();
                } else if (type == PacketType.Play.Client.PLAYER_POSITION_AND_ROTATION) {
                    WrapperPlayClientPlayerPositionAndRotation wrapper = new WrapperPlayClientPlayerPositionAndRotation(event);
                    px = wrapper.getLocation().getX(); py = wrapper.getLocation().getY(); pz = wrapper.getLocation().getZ();
                    pYaw = wrapper.getLocation().getYaw(); pPitch = wrapper.getLocation().getPitch();
                } else if (type == PacketType.Play.Client.PLAYER_ROTATION) {
                    WrapperPlayClientPlayerRotation wrapper = new WrapperPlayClientPlayerRotation(event);
                    pYaw = wrapper.getLocation().getYaw(); pPitch = wrapper.getLocation().getPitch();
                }

                boolean hasPosition = !Double.isNaN(px);
                boolean hasRotation = !Float.isNaN(pYaw);

                // Handle target player's own recording
                if (recordingManager.isRecording(playerUuid)) {
                    if (hasPosition || hasRotation) {
                        handlePlayerPosition(playerUuid, playerUuid, px, py, pz, pYaw, pPitch);
                    } else if (type == PacketType.Play.Client.ANIMATION) {
                        WrapperPlayClientAnimation wrapper = new WrapperPlayClientAnimation(event);
                        long now = System.currentTimeMillis();
                        int deltaMs = (int) (now - getRecordingStartTime(playerUuid));
                        PlayerAnimEvent.AnimationType animType = (wrapper.getHand() == InteractionHand.OFF_HAND)
                                ? PlayerAnimEvent.AnimationType.SWING_OFF_ARM
                                : PlayerAnimEvent.AnimationType.SWING_MAIN_ARM;
                        enqueue(playerUuid, new PlayerAnimEvent(deltaMs, playerUuid, animType));
                    }
                }

                // Correct EntityTracker positions for OTHER recordings tracking this player.
                // C2S gives exact absolute coordinates; S2C relative moves handle event emission.
                if (hasPosition || hasRotation) {
                    List<UUID> viewers = entityTracker.findViewersTracking(playerUuid);
                    for (UUID viewerUuid : viewers) {
                        if (viewerUuid.equals(playerUuid)) continue; // already handled above
                        EntityTracker.TrackedEntity entity = entityTracker.getByUuid(viewerUuid, playerUuid);
                        if (entity == null) continue;
                        if (hasPosition) {
                            entity.setX(px);
                            entity.setY(py);
                            entity.setZ(pz);
                        }
                        if (hasRotation) {
                            entity.setYaw(pYaw);
                            entity.setPitch(pPitch);
                        }
                    }
                }
            });
        } catch (Exception e) {
            logger.warning("Error processing client packet " + type + ": " + e.getMessage());
        }
    }

    /**
     * Handle a player's C2S position/rotation and emit into a specific recording.
     * @param viewerUuid the recording target (whose recording this goes into)
     * @param playerUuid the player who sent the C2S packet
     */
    private void handlePlayerPosition(UUID viewerUuid, UUID playerUuid, double x, double y, double z, float yaw, float pitch) {
        // Throttle move events per player
        long now = System.currentTimeMillis();
        Long lastMove = lastSelfMoveTimestamps.get(playerUuid);
        if (lastMove != null && (now - lastMove) < config.moveThrottleMs()) return;
        lastSelfMoveTimestamps.put(playerUuid, now);

        // Update position in EntityTracker
        EntityTracker.TrackedEntity entity = entityTracker.getByUuid(viewerUuid, playerUuid);
        if (entity == null) return;

        boolean hasPosition = !Double.isNaN(x);
        boolean hasRotation = !Float.isNaN(yaw);

        if (hasPosition) {
            entity.setX(x);
            entity.setY(y);
            entity.setZ(z);
        }
        if (hasRotation) {
            entity.setYaw(yaw);
            entity.setPitch(pitch);
        }

        // Update recording bounding box for the target player's own position
        if (viewerUuid.equals(playerUuid) && hasPosition) {
            recordingManager.updatePlayerPosition(viewerUuid, x, y, z);
        }

        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));

        PlayerMoveEvent moveEvent = new PlayerMoveEvent(
                deltaMs, playerUuid,
                (float) entity.getX(), (float) entity.getY(), (float) entity.getZ(),
                entity.getYaw(), entity.getPitch()
        );
        enqueue(viewerUuid, moveEvent);
    }

    /**
     * Handles packets that must be tracked at ALL times (even when not recording).
     * Returns true if the packet was fully handled and no further processing is needed.
     */
    private boolean handleAlwaysTracked(PacketSendEvent event, Object type, UUID viewerUuid) {
        // Track world changes so chunk cache is keyed correctly per-world
        if (type == PacketType.Play.Server.JOIN_GAME) {
            WrapperPlayServerJoinGame wrapper = new WrapperPlayServerJoinGame(event);
            String world = wrapper.getWorldName();
            if (world != null) chunkTracker.setPlayerWorld(viewerUuid, world);
            return false;
        }

        if (type == PacketType.Play.Server.RESPAWN) {
            WrapperPlayServerRespawn wrapper = new WrapperPlayServerRespawn(event);
            wrapper.getWorldName().ifPresent(world -> chunkTracker.setPlayerWorld(viewerUuid, world));
            return false;
        }

        if (type == PacketType.Play.Server.CHUNK_DATA) {
            WrapperPlayServerChunkData wrapper = new WrapperPlayServerChunkData(event);
            Column column = wrapper.getColumn();
            chunkTracker.handleChunkData(viewerUuid, column.getX(), column.getZ(), column);
            return true;
        }

        // 1.8 sends chunk data in bulk via MAP_CHUNK_BULK (removed in 1.9)
        if (type == PacketType.Play.Server.MAP_CHUNK_BULK) {
            WrapperPlayServerChunkDataBulk wrapper = new WrapperPlayServerChunkDataBulk(event);
            int[] xs = wrapper.getX();
            int[] zs = wrapper.getZ();
            BaseChunk[][] allChunks = wrapper.getChunks();
            for (int i = 0; i < xs.length; i++) {
                Column column = new Column(xs[i], zs[i], true, allChunks[i], null);
                chunkTracker.handleChunkData(viewerUuid, xs[i], zs[i], column);
            }
            return true;
        }

        if (type == PacketType.Play.Server.UNLOAD_CHUNK) {
            WrapperPlayServerUnloadChunk wrapper = new WrapperPlayServerUnloadChunk(event);
            chunkTracker.handleChunkUnload(viewerUuid, wrapper.getChunkX(), wrapper.getChunkZ());
            return true;
        }

        // Always track player info so names are available when recording starts
        if (type == PacketType.Play.Server.PLAYER_INFO_UPDATE) {
            handlePlayerInfo(event, viewerUuid);
            return true;
        }

        // Always track entity spawns so we know about entities before recording starts
        if (type == PacketType.Play.Server.SPAWN_ENTITY) {
            handleSpawnEntity(event, viewerUuid);
            return true;
        }

        // Always track entity removals
        if (type == PacketType.Play.Server.DESTROY_ENTITIES) {
            handleDestroyEntities(event, viewerUuid);
            return true;
        }

        // Always cache inventory state so it's populated when recording starts.
        // Don't return true — let these fall through to handleRecordingPacket for event emission.
        if (type == PacketType.Play.Server.WINDOW_ITEMS) {
            cacheWindowItems(event, viewerUuid);
        } else if (type == PacketType.Play.Server.SET_SLOT) {
            cacheSetSlot(event, viewerUuid);
        } else if (type == PacketType.Play.Server.HELD_ITEM_CHANGE) {
            cacheHeldItemChange(event, viewerUuid);
        }

        // Track item pickups (COLLECT_ITEM) even when not recording
        if (type == PacketType.Play.Server.COLLECT_ITEM) {
            WrapperPlayServerCollectItem wrapper = new WrapperPlayServerCollectItem(event);
            entityTracker.removeEntity(viewerUuid, wrapper.getCollectedEntityId());
            // Don't return — let it fall through to handleRecordingPacket for event emission
        }

        // Cross-player equipment sync: when player A sees ENTITY_EQUIPMENT about player B,
        // and player B is a recording target, update B's equipment cache.
        // This catches right-click armor equipping which doesn't send SET_SLOT to self.
        if (type == PacketType.Play.Server.ENTITY_EQUIPMENT) {
            handleCrossPlayerEquipment(event, viewerUuid);
            // Don't return — let it fall through to handleRecordingPacket for normal processing
        }

        // Always track entity movement for position accuracy
        if (type == PacketType.Play.Server.ENTITY_RELATIVE_MOVE) {
            handleRelativeMove(event, viewerUuid);
            return true;
        }

        if (type == PacketType.Play.Server.ENTITY_RELATIVE_MOVE_AND_ROTATION) {
            handleRelativeMoveAndRotation(event, viewerUuid);
            return true;
        }

        if (type == PacketType.Play.Server.ENTITY_TELEPORT) {
            handleEntityTeleport(event, viewerUuid);
            return true;
        }

        return false;
    }

    /**
     * Handles packets that produce replay events (only when recording is active).
     */
    private void handleRecordingPacket(PacketSendEvent event, Object type, UUID viewerUuid) {
        // --- Entity Animation ---
        if (type == PacketType.Play.Server.ENTITY_ANIMATION) {
            handleEntityAnimation(event, viewerUuid);
            return;
        }

        // --- Hurt Animation (1.19.4+ damage indicator for other players) ---
        if (type == PacketType.Play.Server.HURT_ANIMATION) {
            handleHurtAnimation(event, viewerUuid);
            return;
        }

        // --- Entity Metadata (sneak detection) ---
        if (type == PacketType.Play.Server.ENTITY_METADATA) {
            handleEntityMetadata(event, viewerUuid);
            return;
        }

        // --- Equipment ---
        if (type == PacketType.Play.Server.ENTITY_EQUIPMENT) {
            handleEntityEquipment(event, viewerUuid);
            return;
        }

        // --- Health ---
        if (type == PacketType.Play.Server.UPDATE_HEALTH) {
            handleUpdateHealth(event, viewerUuid);
            return;
        }

        // --- Inventory ---
        if (type == PacketType.Play.Server.WINDOW_ITEMS) {
            handleWindowItems(event, viewerUuid);
            return;
        }

        if (type == PacketType.Play.Server.SET_SLOT) {
            handleSetSlot(event, viewerUuid);
            return;
        }

        // --- Held Item Change (self-equipment tracking) ---
        if (type == PacketType.Play.Server.HELD_ITEM_CHANGE) {
            handleHeldItemChange(event, viewerUuid);
            return;
        }

        // --- Block Changes ---
        if (type == PacketType.Play.Server.BLOCK_CHANGE) {
            handleBlockChange(event, viewerUuid);
            return;
        }

        if (type == PacketType.Play.Server.MULTI_BLOCK_CHANGE) {
            handleMultiBlockChange(event, viewerUuid);
            return;
        }

        // --- Chat ---
        if (type == PacketType.Play.Server.SYSTEM_CHAT_MESSAGE) {
            handleSystemChat(event, viewerUuid);
            return;
        }

        // --- Collect Item (pickup) ---
        if (type == PacketType.Play.Server.COLLECT_ITEM) {
            handleCollectItem(event, viewerUuid);
            return;
        }

        // --- Entity Effect ---
        if (type == PacketType.Play.Server.ENTITY_EFFECT) {
            handleEntityEffect(event, viewerUuid);
        }
    }

    // ==================== Spawn Handlers ====================

    private void handleSpawnEntity(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerSpawnEntity wrapper = new WrapperPlayServerSpawnEntity(event);

        handleSpawnEntity(
                viewerUuid,
                wrapper.getEntityId(),
                wrapper.getUUID().orElse(null),
                wrapper.getEntityType(),
                wrapper.getPosition().getX(),
                wrapper.getPosition().getY(),
                wrapper.getPosition().getZ(),
                wrapper.getYaw(),
                wrapper.getPitch(),
                wrapper.getData(),
                eventClientVersion(event)
        );
    }

    private void handleSpawnEntity(UUID viewerUuid, int entityId, UUID entityUuid, EntityType entityType,
                                   double x, double y, double z, float yaw, float pitch, int data,
                                   ClientVersion version) {
        SpawnEntityClassifier.SpawnKind spawnKind =
                SpawnEntityClassifier.classify(entityTracker.isKnownPlayer(entityUuid), entityType);

        if (spawnKind == SpawnEntityClassifier.SpawnKind.PLAYER) {
            // Use global name cache — PLAYER_INFO_UPDATE arrives before SPAWN_ENTITY
            String name = entityTracker.getPlayerName(entityUuid);

            // Check if already tracked (e.g. seeded via Bukkit API initial spawns)
            EntityTracker.TrackedEntity existing = entityTracker.getByUuid(viewerUuid, entityUuid);
            if (existing != null) {
                // Already known — just update entityId and position
                entityTracker.updateEntityId(viewerUuid, entityUuid, entityId);
                entityTracker.updatePosition(viewerUuid, entityId, x, y, z, yaw, pitch);
            } else {
                entityTracker.trackPlayer(viewerUuid, entityId, entityUuid, name, x, y, z, yaw, pitch);
            }

            if (recordingManager.isRecording(viewerUuid)) {
                long now = System.currentTimeMillis();
                PlayerSpawnEvent spawnEvent = new PlayerSpawnEvent(
                        (int) (now - getRecordingStartTime(viewerUuid)),
                        entityUuid, name, (float) x, (float) y, (float) z,
                        yaw, pitch, new byte[0]
                );
                enqueue(viewerUuid, spawnEvent);
            }
        } else if (spawnKind == SpawnEntityClassifier.SpawnKind.NON_PLAYER) {
            if (entityType == null) {
                logger.warning("Unresolved entity type for NON_PLAYER! entityId=" + entityId + ", entityUuid=" + entityUuid);
                return; 
            }
            int typeId = entityType.getId(version);
            boolean noThrottleMovement = isNoThrottleType(entityType, version);
            entityTracker.trackEntity(viewerUuid, entityId, entityUuid, false, typeId, noThrottleMovement, x, y, z, yaw, pitch);

            if (recordingManager.isRecording(viewerUuid)) {
                long now = System.currentTimeMillis();
                // Send entity type name as metadata so the viewer can identify the entity.
                // For falling blocks, append ":stateId" so the viewer knows which block texture to use.
                String typeName = entityType.getName().toString();
                if (typeName.startsWith("minecraft:")) typeName = typeName.substring(10);
                if (entityType == EntityTypes.FALLING_BLOCK) {
                    int blockStateId = data;
                    typeName = typeName + ":" + blockStateId;
                }
                byte[] metadata = typeName.getBytes(StandardCharsets.UTF_8);
                EntitySpawnEvent spawnEvent = new EntitySpawnEvent(
                        (int) (now - getRecordingStartTime(viewerUuid)),
                        entityId, (short) typeId, (float) x, (float) y, (float) z, metadata
                );
                enqueue(viewerUuid, spawnEvent);
            }
        } else {
            logger.warning("Skipping SPAWN_ENTITY for unresolved entity type, entityId=" + entityId
                    + ", entityUuid=" + entityUuid);
        }
    }

    private void handlePlayerInfo(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerPlayerInfoUpdate wrapper = new WrapperPlayServerPlayerInfoUpdate(event);

        for (WrapperPlayServerPlayerInfoUpdate.PlayerInfo entry : wrapper.getEntries()) {
            UserProfile profile = entry.getGameProfile();
            if (profile != null && profile.getUUID() != null && profile.getName() != null) {
                // Cache name globally — independent of entity lifecycle
                entityTracker.cachePlayerName(profile.getUUID(), profile.getName());

                // Also update existing tracked entity if already spawned for this viewer
                EntityTracker.TrackedEntity tracked = entityTracker.getByUuid(viewerUuid, profile.getUUID());
                if (tracked != null) {
                    tracked.setPlayerName(profile.getName());
                }

                // Extract and record skin texture if recording
                if (recordingManager.isRecording(viewerUuid)) {
                    UUID entityUuid = profile.getUUID();
                    Set<UUID> recorded = skinRecorded.computeIfAbsent(viewerUuid, k -> ConcurrentHashMap.newKeySet());
                    if (recorded.add(entityUuid)) {
                        // First time seeing this player's skin for this recording
                        extractAndDownloadSkin(profile, viewerUuid, entityUuid);
                    }
                }
            }
        }
    }

    private void extractAndDownloadSkin(UserProfile profile, UUID viewerUuid, UUID entityUuid) {
        List<TextureProperty> textures = profile.getTextureProperties();
        if (textures == null) return;

        for (TextureProperty prop : textures) {
            if (!"textures".equals(prop.getName())) continue;

            try {
                String decoded = new String(Base64.getDecoder().decode(prop.getValue()));
                JsonObject json = JsonParser.parseString(decoded).getAsJsonObject();
                JsonObject texturesObj = json.getAsJsonObject("textures");
                if (texturesObj == null) continue;
                JsonObject skinObj = texturesObj.getAsJsonObject("SKIN");
                if (skinObj == null) continue;
                String skinUrl = skinObj.get("url").getAsString();

                CompletableFuture.runAsync(() -> {
                    byte[] png = downloadSkinPng(skinUrl);
                    if (png != null && png.length > 0 && recordingManager.isRecording(viewerUuid)) {
                        long now = System.currentTimeMillis();
                        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));
                        enqueue(viewerUuid, new PlayerSkinEvent(deltaMs, entityUuid, png));
                    }
                }, skinDownloadExecutor);
            } catch (Exception e) {
                logger.warning("Failed to extract skin texture for " + entityUuid + ": " + e.getMessage());
            }
            break;
        }
    }

    private byte[] downloadSkinPng(String url) {
        try {
            HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(SKIN_TIMEOUT_MS);
            conn.setReadTimeout(SKIN_TIMEOUT_MS);
            if (conn.getResponseCode() == 200) {
                try (InputStream is = conn.getInputStream();
                     ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = is.read(buf)) != -1) bos.write(buf, 0, n);
                    return bos.toByteArray();
                }
            }
        } catch (Exception e) {
            logger.warning("Failed to download skin from " + url + ": " + e.getMessage());
        }
        return null;
    }

    // ==================== Movement Handlers ====================

    private void handleRelativeMove(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerEntityRelativeMove wrapper = new WrapperPlayServerEntityRelativeMove(event);
        int entityId = wrapper.getEntityId();
        double dx = wrapper.getDeltaX();
        double dy = wrapper.getDeltaY();
        double dz = wrapper.getDeltaZ();

        entityTracker.updatePositionRelative(viewerUuid, entityId, dx, dy, dz);

        if (recordingManager.isRecording(viewerUuid)) {
            emitMoveEvent(entityId, viewerUuid);
        }
    }

    private void handleRelativeMoveAndRotation(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerEntityRelativeMoveAndRotation wrapper =
                new WrapperPlayServerEntityRelativeMoveAndRotation(event);
        int entityId = wrapper.getEntityId();
        double dx = wrapper.getDeltaX();
        double dy = wrapper.getDeltaY();
        double dz = wrapper.getDeltaZ();
        float yaw = wrapper.getYaw();
        float pitch = wrapper.getPitch();

        entityTracker.updatePositionRelative(viewerUuid, entityId, dx, dy, dz, yaw, pitch);

        if (recordingManager.isRecording(viewerUuid)) {
            emitMoveEvent(entityId, viewerUuid);
        }
    }

    private void handleEntityTeleport(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerEntityTeleport wrapper = new WrapperPlayServerEntityTeleport(event);
        int entityId = wrapper.getEntityId();
        double x = wrapper.getPosition().getX();
        double y = wrapper.getPosition().getY();
        double z = wrapper.getPosition().getZ();
        float yaw = wrapper.getYaw();
        float pitch = wrapper.getPitch();

        entityTracker.updatePosition(viewerUuid, entityId, x, y, z, yaw, pitch);

        if (recordingManager.isRecording(viewerUuid)) {
            emitMoveEvent(entityId, viewerUuid);
        }
    }

    private void emitMoveEvent(int entityId, UUID viewerUuid) {
        EntityTracker.TrackedEntity entity = entityTracker.getByEntityId(viewerUuid, entityId);
        if (entity == null) return;

        // Throttle movement events (skip throttle for players and fast-moving entities like projectiles/items)
        long now = System.currentTimeMillis();
        if (!entity.isPlayer() && !entity.isNoThrottleMovement()) {
            int throttleMs = config.moveThrottleMs();
            Map<Integer, Long> timestamps = lastMoveTimestamps.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>());
            Long lastMove = timestamps.get(entityId);
            if (lastMove != null && (now - lastMove) < throttleMs) return;
            timestamps.put(entityId, now);
        }

        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));

        if (entity.isPlayer()) {
            PlayerMoveEvent moveEvent = new PlayerMoveEvent(
                    deltaMs, entity.getUuid(),
                    (float) entity.getX(), (float) entity.getY(), (float) entity.getZ(),
                    entity.getYaw(), entity.getPitch()
            );
            enqueue(viewerUuid, moveEvent);
        } else {
            EntityMoveEvent moveEvent = new EntityMoveEvent(
                    deltaMs, entity.getEntityId(),
                    (float) entity.getX(), (float) entity.getY(), (float) entity.getZ(),
                    entity.getYaw(), entity.getPitch()
            );
            enqueue(viewerUuid, moveEvent);
        }
    }

    // ==================== Remove Handlers ====================

    private void handleDestroyEntities(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerDestroyEntities wrapper = new WrapperPlayServerDestroyEntities(event);

        boolean isRecording = recordingManager.isRecording(viewerUuid);
        long now = isRecording ? System.currentTimeMillis() : 0;
        int deltaMs = isRecording ? (int) (now - getRecordingStartTime(viewerUuid)) : 0;

        for (int entityId : wrapper.getEntityIds()) {
            EntityTracker.TrackedEntity entity = entityTracker.getByEntityId(viewerUuid, entityId);
            if (entity == null) continue;

            if (isRecording) {
                if (entity.isPlayer()) {
                    enqueue(viewerUuid, new PlayerRemoveEvent(deltaMs, entity.getUuid()));
                } else {
                    enqueue(viewerUuid, new EntityRemoveEvent(deltaMs, entityId));
                }
            }

            entityTracker.removeEntity(viewerUuid, entityId);
        }
    }

    // ==================== Collect Item Handler ====================

    private void handleCollectItem(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerCollectItem wrapper = new WrapperPlayServerCollectItem(event);
        int collectedEntityId = wrapper.getCollectedEntityId();

        // Entity was already removed from tracker in handleAlwaysTracked.
        // Emit EntityRemoveEvent so the viewer removes the item from the world.
        long now = System.currentTimeMillis();
        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));
        enqueue(viewerUuid, new EntityRemoveEvent(deltaMs, collectedEntityId));
    }

    // ==================== Animation Handlers ====================

    private void handleEntityAnimation(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerEntityAnimation wrapper = new WrapperPlayServerEntityAnimation(event);
        int entityId = wrapper.getEntityId();

        EntityTracker.TrackedEntity entity = entityTracker.getByEntityId(viewerUuid, entityId);
        if (entity == null || !entity.isPlayer()) return;

        WrapperPlayServerEntityAnimation.EntityAnimationType animType = wrapper.getType();
        PlayerAnimEvent.AnimationType replayAnim;

        switch (animType) {
            case SWING_MAIN_ARM:
                replayAnim = PlayerAnimEvent.AnimationType.SWING_MAIN_ARM;
                break;
            case SWING_OFF_HAND:
                replayAnim = PlayerAnimEvent.AnimationType.SWING_OFF_ARM;
                break;
            case CRITICAL_HIT:
            case MAGIC_CRITICAL_HIT:
                return; // Not tracked in replay format
            case HURT:
                replayAnim = PlayerAnimEvent.AnimationType.DAMAGE;
                break;
            default:
                return;
        }

        long now = System.currentTimeMillis();
        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));
        enqueue(viewerUuid, new PlayerAnimEvent(deltaMs, entity.getUuid(), replayAnim));
    }

    private void handleHurtAnimation(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerHurtAnimation wrapper = new WrapperPlayServerHurtAnimation(event);
        int entityId = wrapper.getEntityId();

        EntityTracker.TrackedEntity entity = entityTracker.getByEntityId(viewerUuid, entityId);
        if (entity == null || !entity.isPlayer()) return;

        long now = System.currentTimeMillis();
        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));
        enqueue(viewerUuid, new PlayerAnimEvent(deltaMs, entity.getUuid(), PlayerAnimEvent.AnimationType.DAMAGE));
    }

    private void handleEntityMetadata(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerEntityMetadata wrapper;
        int entityId;
        List<EntityData<?>> metadata;
        try {
            wrapper = new WrapperPlayServerEntityMetadata(event);
            entityId = wrapper.getEntityId();
            metadata = wrapper.getEntityMetadata();
        } catch (RuntimeException e) {
            if (PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(e)
                    || PacketDecodeFailures.isUnsupportedEntityMetadataDecodeFailure(e)) return;
            throw e;
        }

        EntityTracker.TrackedEntity entity = entityTracker.getByEntityId(viewerUuid, entityId);
        if (entity == null) return;

        if (entity.isPlayer()) {
            // Check for sneak state changes via entity metadata index 0 (shared flags)
            for (EntityData<?> data : metadata) {
                if (data.getIndex() == 0 && data.getValue() instanceof Byte) {
                    Byte flags = (Byte) data.getValue();
                    boolean sneaking = (flags & 0x02) != 0;
                    Map<Integer, Boolean> states = sneakStates.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>());
                    Boolean prev = states.put(entityId, sneaking);

                    if (prev == null || prev != sneaking) {
                        long now = System.currentTimeMillis();
                        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));
                        PlayerAnimEvent.AnimationType animType = sneaking
                                ? PlayerAnimEvent.AnimationType.SNEAK_START
                                : PlayerAnimEvent.AnimationType.SNEAK_STOP;
                        enqueue(viewerUuid, new PlayerAnimEvent(deltaMs, entity.getUuid(), animType));
                    }
                }
            }
        } else if (recordingManager.isRecording(viewerUuid)) {
            // For item entities, metadata index 8 contains the ItemStack.
            // Re-emit a spawn event with "item:<itemname>" so the viewer can texture it.
            int itemTypeId = EntityTypes.ITEM.getId(eventClientVersion(event));
            if (entity.getTypeId() == itemTypeId) {
                for (EntityData<?> data : metadata) {
                    if (data.getIndex() == 8 && data.getValue() instanceof ItemStack) {
                        ItemStack itemStack = (ItemStack) data.getValue();
                        String itemName = getItemName(itemStack);
                        if (!itemName.equals("air")) {
                            String typeName = "item:" + itemName;
                            byte[] spawnMetadata = typeName.getBytes(StandardCharsets.UTF_8);
                            long now = System.currentTimeMillis();
                            int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));
                            EntitySpawnEvent spawnEvent = new EntitySpawnEvent(
                                    deltaMs,
                                    entityId,
                                    (short) itemTypeId,
                                    (float) entity.getX(),
                                    (float) entity.getY(),
                                    (float) entity.getZ(),
                                    spawnMetadata
                            );
                            enqueue(viewerUuid, spawnEvent);
                        }
                    }
                }
            }
        }
    }

    // ==================== Equipment Handler ====================

    private void handleEntityEquipment(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerEntityEquipment wrapper;
        int entityId;
        List<Equipment> equipmentList;
        try {
            wrapper = new WrapperPlayServerEntityEquipment(event);
            entityId = wrapper.getEntityId();
            equipmentList = wrapper.getEquipment();
        } catch (RuntimeException e) {
            if (PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(e)) return;
            throw e;
        }

        EntityTracker.TrackedEntity entity = entityTracker.getByEntityId(viewerUuid, entityId);
        if (entity == null || !entity.isPlayer()) return;

        if (equipmentList == null || equipmentList.isEmpty()) return;

        Map<Integer, String> equipmentUpdates = new HashMap<>();
        Map<Integer, List<PlayerEquipmentFullEvent.EnchantEntry>> enchantCache = new HashMap<>();

        try {
            for (Equipment equipment : equipmentList) {
                int slotId = mapEquipmentSlot(equipment.getSlot());
                String itemName = getItemName(equipment.getItem());
                equipmentUpdates.put(slotId, itemName);
                List<PlayerInventoryEvent.EnchantEntry> invEnchants = getEnchantments(equipment.getItem(), eventClientVersion(event));
                enchantCache.put(slotId, toEquipEnchants(invEnchants));
            }
        } catch (RuntimeException e) {
            if (PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(e)) return;
            throw e;
        }

        // Update cached equipment for this entity only after item decoding succeeds.
        Map<Integer, String> cache = otherPlayerEquipment
                .computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(entity.getUuid(), k -> new ConcurrentHashMap<>());
        cache.putAll(equipmentUpdates);

        // Emit full equipment snapshot from cache
        long now = System.currentTimeMillis();
        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));

        List<PlayerEquipmentFullEvent.SlotEntry> slots = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : cache.entrySet()) {
            List<PlayerEquipmentFullEvent.EnchantEntry> enchants = enchantCache.getOrDefault(entry.getKey(), Collections.emptyList());
            slots.add(new PlayerEquipmentFullEvent.SlotEntry(entry.getKey(), entry.getValue(), enchants));
        }

        enqueue(viewerUuid, new PlayerEquipmentFullEvent(deltaMs, entity.getUuid(), slots));
    }

    /**
     * Cross-player equipment sync: when player A sees ENTITY_EQUIPMENT about player B,
     * and B is a recording target, update B's inventory cache and emit equipment.
     * Catches right-click armor equipping which doesn't send SET_SLOT to self.
     */
    private void handleCrossPlayerEquipment(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerEntityEquipment wrapper;
        int entityId;
        List<Equipment> equipmentList;
        try {
            wrapper = new WrapperPlayServerEntityEquipment(event);
            entityId = wrapper.getEntityId();
            equipmentList = wrapper.getEquipment();
        } catch (RuntimeException e) {
            if (PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(e)) return;
            throw e;
        }

        EntityTracker.TrackedEntity entity = entityTracker.getByEntityId(viewerUuid, entityId);
        if (entity == null || !entity.isPlayer()) return;

        UUID playerUuid = entity.getUuid();
        // Only care if the entity is a recording target AND this packet is from another player's connection
        if (playerUuid.equals(viewerUuid)) return; // handled by normal equipment path
        if (!recordingManager.isRecording(playerUuid)) return;

        if (equipmentList == null || equipmentList.isEmpty()) return;

        // Update the TARGET's inventory cache with the equipment data
        Map<Integer, String> inventoryUpdates = new HashMap<>();
        for (Equipment eq : equipmentList) {
            try {
                String itemName = getItemName(eq.getItem());
                int protocolSlot;
                EquipmentSlot eqSlot = eq.getSlot();
                if (eqSlot == EquipmentSlot.MAIN_HAND) {
                    protocolSlot = 36 + heldSlots.getOrDefault(playerUuid, 0);
                } else if (eqSlot == EquipmentSlot.OFF_HAND) {
                    protocolSlot = 45;
                } else if (eqSlot == EquipmentSlot.HELMET) {
                    protocolSlot = 5;
                } else if (eqSlot == EquipmentSlot.CHEST_PLATE) {
                    protocolSlot = 6;
                } else if (eqSlot == EquipmentSlot.LEGGINGS) {
                    protocolSlot = 7;
                } else if (eqSlot == EquipmentSlot.BOOTS) {
                    protocolSlot = 8;
                } else {
                    protocolSlot = -1;
                }
                if (protocolSlot >= 0) {
                    inventoryUpdates.put(protocolSlot, itemName);
                }
            } catch (RuntimeException e) {
                if (PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(e)) return;
                throw e;
            }
        }

        Map<Integer, String> inv = inventoryCache.computeIfAbsent(playerUuid, k -> new ConcurrentHashMap<>());
        inv.putAll(inventoryUpdates);

        // Re-emit self-equipment into the target's recording
        emitSelfEquipment(playerUuid);
    }

    private int mapEquipmentSlot(EquipmentSlot slot) {
        if (slot == EquipmentSlot.MAIN_HAND) return 0;
        if (slot == EquipmentSlot.OFF_HAND) return 1;
        if (slot == EquipmentSlot.HELMET) return 2;
        if (slot == EquipmentSlot.CHEST_PLATE) return 3;
        if (slot == EquipmentSlot.LEGGINGS) return 4;
        if (slot == EquipmentSlot.BOOTS) return 5;
        return 0;
    }

    // ==================== Health Handler ====================

    private void handleUpdateHealth(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerUpdateHealth wrapper = new WrapperPlayServerUpdateHealth(event);

        float health = wrapper.getHealth();
        int food = wrapper.getFood();
        float saturation = wrapper.getFoodSaturation();

        long now = System.currentTimeMillis();
        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));

        enqueue(viewerUuid, new PlayerHealthEvent(deltaMs, viewerUuid, health, (byte) food, saturation));

        // Self-damage detection: emit DAMAGE animation when health decreases
        Float prevHealth = lastHealthValues.put(viewerUuid, health);
        if (prevHealth != null && health < prevHealth && health > 0) {
            enqueue(viewerUuid, new PlayerAnimEvent(deltaMs, viewerUuid, PlayerAnimEvent.AnimationType.DAMAGE));
        }
    }

    // ==================== Slot Index Translation ====================

    /**
     * Translate Minecraft protocol slot indices to viewer-expected Bukkit-style layout.
     * Protocol: 0=crafting output, 1-4=crafting grid, 5-8=armor, 9-35=main inv, 36-44=hotbar, 45=offhand
     * Viewer:   0-8=hotbar, 9-35=main inv, 36-39=armor (36=feet,37=legs,38=chest,39=head), 40=offhand
     * Returns -1 for slots that should be skipped (crafting).
     */
    private int translateSlotIndex(int protocolSlot) {
        if (protocolSlot >= 36 && protocolSlot <= 44) return protocolSlot - 36; // hotbar → 0-8
        if (protocolSlot >= 9 && protocolSlot <= 35) return protocolSlot;       // main inv → 9-35
        if (protocolSlot == 5) return 39; // helmet → head
        if (protocolSlot == 6) return 38; // chestplate → chest
        if (protocolSlot == 7) return 37; // leggings → legs
        if (protocolSlot == 8) return 36; // boots → feet
        if (protocolSlot == 45) return 40; // offhand
        return -1; // crafting slots — skip
    }

    // ==================== Inventory Cache (always-on) ====================

    private void cacheWindowItems(PacketSendEvent event, UUID viewerUuid) {
        Map<Integer, String> inventoryUpdates = new HashMap<>();
        Map<Integer, Integer> countUpdates = new HashMap<>();
        Map<Integer, List<PlayerInventoryEvent.EnchantEntry>> enchantUpdates = new HashMap<>();

        try {
            WrapperPlayServerWindowItems wrapper = new WrapperPlayServerWindowItems(event);
            if (wrapper.getWindowId() != 0) return;
            List<ItemStack> items = wrapper.getItems();
            if (items == null) return;
            for (int i = 0; i < items.size() && i < 46; i++) {
                ItemStack item = items.get(i);
                inventoryUpdates.put(i, getItemName(item));
                countUpdates.put(i, item != null ? item.getAmount() : 0);
                enchantUpdates.put(i, getEnchantments(item, eventClientVersion(event)));
            }
        } catch (RuntimeException e) {
            if (PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(e)) return;
            throw e;
        }

        inventoryCache.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>()).putAll(inventoryUpdates);
        inventoryCountCache.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>()).putAll(countUpdates);
        inventoryEnchantCache.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>()).putAll(enchantUpdates);
    }

    private void cacheSetSlot(PacketSendEvent event, UUID viewerUuid) {
        int slotIndex;
        String itemName;
        int itemAmount;
        List<PlayerInventoryEvent.EnchantEntry> enchants;

        try {
            WrapperPlayServerSetSlot wrapper = new WrapperPlayServerSetSlot(event);
            int windowId = wrapper.getWindowId();
            if (windowId != 0 && windowId != -2) return;
            slotIndex = wrapper.getSlot();
            ItemStack item = wrapper.getItem();
            itemName = getItemName(item);
            itemAmount = item != null ? item.getAmount() : 0;
            enchants = getEnchantments(item, eventClientVersion(event));
        } catch (RuntimeException e) {
            if (PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(e)) return;
            throw e;
        }

        inventoryCache.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>()).put(slotIndex, itemName);
        inventoryCountCache.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>()).put(slotIndex, itemAmount);
        inventoryEnchantCache.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>()).put(slotIndex, enchants);
    }

    private void cacheHeldItemChange(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerHeldItemChange wrapper = new WrapperPlayServerHeldItemChange(event);
        heldSlots.put(viewerUuid, wrapper.getSlot());
    }

    // ==================== Inventory Handlers ====================

    private void handleWindowItems(PacketSendEvent event, UUID viewerUuid) {
        List<PlayerInventoryEvent.SlotEntry> slots = new ArrayList<>();
        Map<Integer, String> inventoryUpdates = new HashMap<>();

        try {
            WrapperPlayServerWindowItems wrapper = new WrapperPlayServerWindowItems(event);
            if (wrapper.getWindowId() != 0) return; // Only player inventory
            List<ItemStack> items = wrapper.getItems();
            if (items == null) return;

            for (int i = 0; i < items.size() && i < 46; i++) {
                ItemStack item = items.get(i);
                String itemName = getItemName(item);
                inventoryUpdates.put(i, itemName);
                int viewerSlot = translateSlotIndex(i);
                if (viewerSlot < 0) continue; // skip crafting slots
                if (item != null && !item.isEmpty()) {
                    slots.add(new PlayerInventoryEvent.SlotEntry(viewerSlot, itemName, item.getAmount(), getEnchantments(item, eventClientVersion(event))));
                }
            }
        } catch (RuntimeException e) {
            if (PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(e)) return;
            throw e;
        }

        long now = System.currentTimeMillis();
        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));
        Map<Integer, String> invCache = inventoryCache.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>());
        invCache.putAll(inventoryUpdates);

        enqueue(viewerUuid, new PlayerInventoryEvent(deltaMs, viewerUuid, true, slots));

        // Emit self-equipment update since inventory changed
        emitSelfEquipment(viewerUuid);
    }

    private void handleSetSlot(PacketSendEvent event, UUID viewerUuid) {
        String itemName;
        int itemAmount = 0;
        int slotIndex;
        int viewerSlot;
        List<PlayerInventoryEvent.EnchantEntry> enchants = Collections.emptyList();

        try {
            WrapperPlayServerSetSlot wrapper = new WrapperPlayServerSetSlot(event);
            int windowId = wrapper.getWindowId();
            if (windowId != 0 && windowId != -2) return; // Only player inventory (0 = normal, -2 = direct)

            slotIndex = wrapper.getSlot();
            ItemStack item = wrapper.getItem();
            viewerSlot = translateSlotIndex(slotIndex);

            if (item != null && !item.isEmpty()) {
                itemName = getItemName(item);
                itemAmount = item.getAmount();
                enchants = getEnchantments(item, eventClientVersion(event));
            } else {
                itemName = "air";
            }
        } catch (RuntimeException e) {
            if (PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(e)) return;
            throw e;
        }

        long now = System.currentTimeMillis();
        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));

        // Update inventory cache (uses protocol slot indices)
        Map<Integer, String> invCache = inventoryCache.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>());
        invCache.put(slotIndex, itemName);

        // Emit inventory event with translated slot index
        if (viewerSlot >= 0) {
            List<PlayerInventoryEvent.SlotEntry> slots = new ArrayList<>();
            if (!itemName.equals("air")) {
                slots.add(new PlayerInventoryEvent.SlotEntry(viewerSlot, itemName, itemAmount, enchants));
            } else {
                slots.add(new PlayerInventoryEvent.SlotEntry(viewerSlot, "air", 0));
            }
            enqueue(viewerUuid, new PlayerInventoryEvent(deltaMs, viewerUuid, false, slots));
        }

        // Check if the changed slot is equipment-relevant and emit self-equipment
        int heldSlot = heldSlots.getOrDefault(viewerUuid, 0);
        if (slotIndex == 5 || slotIndex == 6 || slotIndex == 7 || slotIndex == 8  // armor
                || slotIndex == 45  // off-hand
                || slotIndex == 36 + heldSlot) {  // current main hand
            emitSelfEquipment(viewerUuid);
        }
    }

    // ==================== Held Item Change Handler ====================

    private void handleHeldItemChange(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerHeldItemChange wrapper = new WrapperPlayServerHeldItemChange(event);
        int newSlot = wrapper.getSlot();
        int oldSlot = heldSlots.getOrDefault(viewerUuid, 0);
        heldSlots.put(viewerUuid, newSlot);

        // Emit equipment update if held item changed
        if (oldSlot != newSlot) {
            emitSelfEquipment(viewerUuid);
        }
    }

    /**
     * Emit a PlayerEquipmentFullEvent for the self-player based on cached inventory state.
     */
    private void emitSelfEquipment(UUID viewerUuid) {
        Map<Integer, String> inv = inventoryCache.get(viewerUuid);
        if (inv == null) return;
        int held = heldSlots.getOrDefault(viewerUuid, 0);
        long now = System.currentTimeMillis();
        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));
        Map<Integer, List<PlayerInventoryEvent.EnchantEntry>> enchCache = inventoryEnchantCache.getOrDefault(viewerUuid, Collections.emptyMap());

        List<PlayerEquipmentFullEvent.SlotEntry> slots = buildEquipmentSlots(inv, enchCache, held);
        enqueue(viewerUuid, new PlayerEquipmentFullEvent(deltaMs, viewerUuid, slots));
    }

    private List<PlayerEquipmentFullEvent.SlotEntry> buildEquipmentSlots(
            Map<Integer, String> inv,
            Map<Integer, List<PlayerInventoryEvent.EnchantEntry>> enchCache,
            int held) {
        List<PlayerEquipmentFullEvent.SlotEntry> slots = new ArrayList<>();
        slots.add(equipmentSlot(0, 36 + held, inv, enchCache)); // main hand
        slots.add(equipmentSlot(1, 45, inv, enchCache));        // off hand
        slots.add(equipmentSlot(2, 5, inv, enchCache));         // helmet (HEAD)
        slots.add(equipmentSlot(3, 6, inv, enchCache));         // chestplate (CHEST)
        slots.add(equipmentSlot(4, 7, inv, enchCache));         // leggings (LEGS)
        slots.add(equipmentSlot(5, 8, inv, enchCache));         // boots (FEET)
        return slots;
    }

    private PlayerEquipmentFullEvent.SlotEntry equipmentSlot(
            int replaySlot,
            int protocolSlot,
            Map<Integer, String> inv,
            Map<Integer, List<PlayerInventoryEvent.EnchantEntry>> enchCache) {
        return new PlayerEquipmentFullEvent.SlotEntry(
                replaySlot,
                inv.getOrDefault(protocolSlot, "air"),
                toEquipEnchants(enchCache.get(protocolSlot))
        );
    }

    private List<PlayerEquipmentFullEvent.EnchantEntry> toEquipEnchants(List<PlayerInventoryEvent.EnchantEntry> invEnchants) {
        if (invEnchants == null || invEnchants.isEmpty()) return Collections.emptyList();
        List<PlayerEquipmentFullEvent.EnchantEntry> result = new ArrayList<>();
        for (PlayerInventoryEvent.EnchantEntry e : invEnchants) {
            result.add(new PlayerEquipmentFullEvent.EnchantEntry(e.getEnchantId(), e.getLevel()));
        }
        return result;
    }

    // ==================== Block Change Handlers ====================

    private void handleBlockChange(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerBlockChange wrapper = new WrapperPlayServerBlockChange(event);

        int x = wrapper.getBlockPosition().getX();
        int y = wrapper.getBlockPosition().getY();
        int z = wrapper.getBlockPosition().getZ();
        int stateId = chunkTracker.translateStateId(wrapper.getBlockId());

        long now = System.currentTimeMillis();
        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));

        enqueue(viewerUuid, new BlockChangeEvent(deltaMs, x, (short) y, z, stateId));
    }

    private void handleMultiBlockChange(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerMultiBlockChange wrapper = new WrapperPlayServerMultiBlockChange(event);

        long now = System.currentTimeMillis();
        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));

        for (WrapperPlayServerMultiBlockChange.EncodedBlock block : wrapper.getBlocks()) {
            int x = block.getX();
            int y = block.getY();
            int z = block.getZ();
            int stateId = chunkTracker.translateStateId(block.getBlockId());
            enqueue(viewerUuid, new BlockChangeEvent(deltaMs, x, (short) y, z, stateId));
        }
    }

    // ==================== Chat Handler ====================

    private void handleSystemChat(PacketSendEvent event, UUID viewerUuid) {
        String message;
        try {
            WrapperPlayServerSystemChatMessage wrapper = new WrapperPlayServerSystemChatMessage(event);
            message = wrapper.getMessageJson();
        } catch (Exception e) {
            // PacketEvents can fail to deserialize chat messages containing NBT data components
            // (e.g. item hover text with custom_data). Skip these non-critical messages.
            return;
        }
        if (message == null || message.isEmpty()) return;

        long now = System.currentTimeMillis();
        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));

        UUID nilUuid = new UUID(0, 0);
        enqueue(viewerUuid, new ChatEvent(deltaMs, nilUuid, message));
    }

    // ==================== Effect Handler ====================

    private void handleEntityEffect(PacketSendEvent event, UUID viewerUuid) {
        WrapperPlayServerEntityEffect wrapper = new WrapperPlayServerEntityEffect(event);

        int entityId = wrapper.getEntityId();
        EntityTracker.TrackedEntity entity = entityTracker.getByEntityId(viewerUuid, entityId);
        if (entity == null || !entity.isPlayer()) return;

        int effectTypeId = wrapper.getPotionType().getId(eventClientVersion(event));
        int amplifier = wrapper.getEffectAmplifier();
        int durationTicks = wrapper.getEffectDurationTicks();

        long now = System.currentTimeMillis();
        int deltaMs = (int) (now - getRecordingStartTime(viewerUuid));

        List<PlayerEffectsEvent.EffectEntry> effects = new ArrayList<>();
        effects.add(new PlayerEffectsEvent.EffectEntry(effectTypeId, amplifier, durationTicks));
        enqueue(viewerUuid, new PlayerEffectsEvent(deltaMs, entity.getUuid(), effects));
    }

    /**
     * Generate initial PlayerSpawnEvents for all players already visible to the target.
     * Called before startRecording() so these events are written as pre-roll at deltaMs=0.
     */
    public List<ReplayEvent> generateInitialPlayerSpawns(UUID viewerUuid) {
        List<ReplayEvent> events = new ArrayList<>();
        List<EntityTracker.TrackedEntity> visiblePlayers = entityTracker.getVisiblePlayers(viewerUuid);

        for (EntityTracker.TrackedEntity player : visiblePlayers) {
            // Skip the target player themselves
            if (player.getUuid().equals(viewerUuid)) continue;

            String name = entityTracker.getPlayerName(player.getUuid());
            PlayerSpawnEvent spawnEvent = new PlayerSpawnEvent(
                    0, // deltaMs = 0 for initial snapshot
                    player.getUuid(), name,
                    (float) player.getX(), (float) player.getY(), (float) player.getZ(),
                    player.getYaw(), player.getPitch(),
                    new byte[0]
            );
            events.add(spawnEvent);
        }

        return events;
    }

    /**
     * Seed the inventory cache for a player from external data (e.g., Bukkit API).
     * Uses protocol slot indices (0=crafting out, 5-8=armor, 9-35=main inv, 36-44=hotbar, 45=offhand).
     * Should be called before emitInitialSelfEquipment() when starting a recording
     * to ensure equipment/inventory is available even if no WINDOW_ITEMS packet has been received yet.
     */
    public void seedInventoryCache(UUID playerUuid, Map<Integer, String> protocolSlotItems, Map<Integer, Integer> protocolSlotCounts) {
        Map<Integer, String> invCache = inventoryCache.computeIfAbsent(playerUuid, k -> new ConcurrentHashMap<>());
        invCache.putAll(protocolSlotItems);
        if (protocolSlotCounts != null) {
            Map<Integer, Integer> countCache = inventoryCountCache.computeIfAbsent(playerUuid, k -> new ConcurrentHashMap<>());
            countCache.putAll(protocolSlotCounts);
        }
    }

    /**
     * Set the held slot index for a player from external data.
     * Should be called alongside seedInventoryCache to ensure correct main-hand item.
     */
    public void seedHeldSlot(UUID playerUuid, int slot) {
        heldSlots.put(playerUuid, slot);
    }

    /**
     * Emit initial self-equipment based on cached inventory state.
     * Called after startRecording so the replay captures current equipment at t=0.
     */
    public void emitInitialSelfEquipment(UUID viewerUuid) {
        PlayerEquipmentFullEvent event = buildInitialEquipmentEvent(viewerUuid);
        if (event != null) {
            enqueue(viewerUuid, event);
        }
    }

    /**
     * Build an initial self-equipment event from cached inventory state.
     * Returns null if no inventory cache is available.
     * Can be used as an initial event (written at t=0, outside the buffer).
     */
    public PlayerEquipmentFullEvent buildInitialEquipmentEvent(UUID viewerUuid) {
        Map<Integer, String> inv = inventoryCache.get(viewerUuid);
        if (inv == null || inv.isEmpty()) return null;
        int held = heldSlots.getOrDefault(viewerUuid, 0);
        Map<Integer, List<PlayerInventoryEvent.EnchantEntry>> enchCache = inventoryEnchantCache.getOrDefault(viewerUuid, Collections.emptyMap());

        List<PlayerEquipmentFullEvent.SlotEntry> slots = buildEquipmentSlots(inv, enchCache, held);
        return new PlayerEquipmentFullEvent(0, viewerUuid, slots);
    }

    /**
     * Build an initial inventory event from cached inventory state.
     * Returns null if no inventory cache is available.
     */
    public PlayerInventoryEvent buildInitialInventoryEvent(UUID viewerUuid) {
        Map<Integer, String> inv = inventoryCache.get(viewerUuid);
        if (inv == null || inv.isEmpty()) return null;
        Map<Integer, Integer> counts = inventoryCountCache.getOrDefault(viewerUuid, Collections.emptyMap());
        Map<Integer, List<PlayerInventoryEvent.EnchantEntry>> enchCache = inventoryEnchantCache.getOrDefault(viewerUuid, Collections.emptyMap());

        List<PlayerInventoryEvent.SlotEntry> slots = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : inv.entrySet()) {
            int viewerSlot = translateSlotIndex(entry.getKey());
            if (viewerSlot < 0) continue;
            String itemName = entry.getValue();
            int count = counts.getOrDefault(entry.getKey(), 1);
            if (itemName != null && !itemName.equals("air") && count > 0) {
                List<PlayerInventoryEvent.EnchantEntry> enchants = enchCache.getOrDefault(entry.getKey(), Collections.emptyList());
                slots.add(new PlayerInventoryEvent.SlotEntry(viewerSlot, itemName, count, enchants));
            }
        }
        if (slots.isEmpty()) return null;
        return new PlayerInventoryEvent(0, viewerUuid, true, slots);
    }

    /**
     * Clean up per-player recording state when a recording stops.
     * Does NOT clear entity/chunk trackers — those remain valid if recording restarts.
     */
    public void cleanupPlayer(UUID playerUuid) {
        lastMoveTimestamps.remove(playerUuid);
        lastSelfMoveTimestamps.remove(playerUuid);
        sneakStates.remove(playerUuid);
        lastHealthValues.remove(playerUuid);
        skinRecorded.remove(playerUuid);
        // Note: heldSlots and inventoryCache are NOT cleared here —
        // they persist between recordings so equipment state is available at next recording start.
    }

    /**
     * Full cleanup when a player disconnects. Clears all per-player state
     * including entity/chunk trackers. Must be called to prevent memory leaks.
     */
    public void disconnectPlayer(UUID playerUuid) {
        cleanupPlayer(playerUuid);
        heldSlots.remove(playerUuid);
        inventoryCache.remove(playerUuid);
        inventoryCountCache.remove(playerUuid);
        inventoryEnchantCache.remove(playerUuid);
        otherPlayerEquipment.remove(playerUuid);
        chunkTracker.clearPlayer(playerUuid);
        entityTracker.clearPlayer(playerUuid);
    }

    // ==================== Utilities ====================

    private static UUID resolvePacketPlayerUuid(User user, Object player) {
        UUID userUuid = user != null ? user.getUUID() : null;
        return PacketPlayerUuidResolver.resolve(userUuid, player);
    }

    private ClientVersion eventClientVersion(PacketSendEvent event) {
        User user = event.getUser();
        if (user != null && user.getClientVersion() != null) {
            return user.getClientVersion();
        }
        if (event.getServerVersion() != null) {
            return event.getServerVersion().toClientVersion();
        }
        return configuredClientVersion;
    }

    static boolean isNoThrottleType(EntityType entityType, ClientVersion version) {
        if (entityType == null) {
            return false;
        }
        return isNoThrottleTypeId(entityType.getId(version), version);
    }

    private static boolean isNoThrottleTypeId(int entityTypeId, ClientVersion version) {
        return entityTypeId == EntityTypes.ENDER_PEARL.getId(version)
                || entityTypeId == EntityTypes.ARROW.getId(version)
                || entityTypeId == EntityTypes.SPECTRAL_ARROW.getId(version)
                || entityTypeId == EntityTypes.SNOWBALL.getId(version)
                || entityTypeId == EntityTypes.TRIDENT.getId(version)
                || entityTypeId == EntityTypes.ITEM.getId(version)
                || entityTypeId == EntityTypes.EGG.getId(version)
                || entityTypeId == EntityTypes.FIREBALL.getId(version)
                || entityTypeId == EntityTypes.SMALL_FIREBALL.getId(version)
                || entityTypeId == EntityTypes.EXPERIENCE_BOTTLE.getId(version)
                || entityTypeId == EntityTypes.POTION.getId(version);
    }

    private List<PlayerInventoryEvent.EnchantEntry> getEnchantments(ItemStack item, ClientVersion version) {
        if (item == null || item.isEmpty()) return Collections.emptyList();
        try {
            List<Enchantment> enchants = item.getEnchantments(version);
            if (enchants == null || enchants.isEmpty()) return Collections.emptyList();
            List<PlayerInventoryEvent.EnchantEntry> result = new ArrayList<>();
            for (Enchantment ench : enchants) {
                String id = ench.getType().getName().toString();
                if (id.startsWith("minecraft:")) id = id.substring(10);
                result.add(new PlayerInventoryEvent.EnchantEntry(id, ench.getLevel()));
            }
            return result;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private String getItemName(ItemStack item) {
        if (item == null || item.isEmpty()) return "air";
        if (item.getType() != null) {
            String name = item.getType().getName().toString();
            // Strip "minecraft:" namespace prefix — viewer expects bare names like "diamond_sword"
            if (name.startsWith("minecraft:")) {
                name = name.substring("minecraft:".length());
            }
            return name;
        }
        return "unknown";
    }

    public long getRecordingStartTime(UUID viewerUuid) {
        RecordingManager.ActiveRecording recording = getRecording(viewerUuid);
        return recording != null ? recording.getStartTime() : System.currentTimeMillis();
    }

    private RecordingManager.ActiveRecording getRecording(UUID viewerUuid) {
        return recordingManager.getRecording(viewerUuid);
    }

    public void enqueue(UUID viewerUuid, ReplayEvent event) {
        recordingManager.enqueueEvent(viewerUuid, event);
    }
}
