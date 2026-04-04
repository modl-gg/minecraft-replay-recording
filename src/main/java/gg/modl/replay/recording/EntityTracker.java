package gg.modl.replay.recording;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks entityId↔UUID mapping and absolute positions for all entities.
 * Uses per-player tracking maps so one player's DESTROY_ENTITIES doesn't affect another's view.
 * Maintains a global player name cache independent of entity lifecycle.
 * Updated from Netty threads via PacketEvents — all methods are thread-safe.
 */
public class EntityTracker {

    // Per-player entity tracking: viewerUuid -> (entityId -> TrackedEntity)
    private final Map<UUID, Map<Integer, TrackedEntity>> perPlayerById = new ConcurrentHashMap<>();
    // Per-player entity tracking by UUID: viewerUuid -> (entityUuid -> TrackedEntity)
    private final Map<UUID, Map<UUID, TrackedEntity>> perPlayerByUuid = new ConcurrentHashMap<>();

    // Global name cache — survives entity remove/re-add cycles
    private final Map<UUID, String> playerNames = new ConcurrentHashMap<>();

    public void trackEntity(UUID viewerUuid, int entityId, UUID uuid, boolean isPlayer, int entityTypeId,
                            double x, double y, double z, float yaw, float pitch) {
        TrackedEntity entity = new TrackedEntity(entityId, uuid, isPlayer, entityTypeId, x, y, z, yaw, pitch);
        perPlayerById.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>()).put(entityId, entity);
        if (uuid != null) {
            perPlayerByUuid.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>()).put(uuid, entity);
        }
    }

    public void trackPlayer(UUID viewerUuid, int entityId, UUID uuid, String playerName,
                            double x, double y, double z, float yaw, float pitch) {
        TrackedEntity entity = new TrackedEntity(entityId, uuid, true, -1, x, y, z, yaw, pitch);
        entity.setPlayerName(playerName);
        perPlayerById.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>()).put(entityId, entity);
        perPlayerByUuid.computeIfAbsent(viewerUuid, k -> new ConcurrentHashMap<>()).put(uuid, entity);
    }

    /**
     * Cache a player's name globally. Called from PLAYER_INFO_UPDATE handler.
     * Independent of entity lifecycle — names persist across spawn/despawn cycles.
     */
    public void cachePlayerName(UUID entityUuid, String name) {
        if (entityUuid != null && name != null) {
            playerNames.put(entityUuid, name);
        }
    }

    /**
     * Get a cached player name, or "Unknown" if not cached.
     */
    public String getPlayerName(UUID entityUuid) {
        return playerNames.getOrDefault(entityUuid, "Unknown");
    }

    /**
     * Check if the given UUID was announced via PLAYER_INFO_UPDATE (i.e. is a player).
     */
    public boolean isKnownPlayer(UUID entityUuid) {
        return entityUuid != null && playerNames.containsKey(entityUuid);
    }

    public void updatePosition(UUID viewerUuid, int entityId, double x, double y, double z, float yaw, float pitch) {
        Map<Integer, TrackedEntity> byId = perPlayerById.get(viewerUuid);
        if (byId == null) return;
        TrackedEntity entity = byId.get(entityId);
        if (entity != null) {
            entity.setX(x);
            entity.setY(y);
            entity.setZ(z);
            entity.setYaw(yaw);
            entity.setPitch(pitch);
        }
    }

    public void updatePositionRelative(UUID viewerUuid, int entityId, double dx, double dy, double dz) {
        Map<Integer, TrackedEntity> byId = perPlayerById.get(viewerUuid);
        if (byId == null) return;
        TrackedEntity entity = byId.get(entityId);
        if (entity != null) {
            entity.setX(entity.getX() + dx);
            entity.setY(entity.getY() + dy);
            entity.setZ(entity.getZ() + dz);
        }
    }

    public void updatePositionRelative(UUID viewerUuid, int entityId, double dx, double dy, double dz, float yaw, float pitch) {
        Map<Integer, TrackedEntity> byId = perPlayerById.get(viewerUuid);
        if (byId == null) return;
        TrackedEntity entity = byId.get(entityId);
        if (entity != null) {
            entity.setX(entity.getX() + dx);
            entity.setY(entity.getY() + dy);
            entity.setZ(entity.getZ() + dz);
            entity.setYaw(yaw);
            entity.setPitch(pitch);
        }
    }

    /**
     * Update the entityId for an already-tracked entity (identified by UUID).
     * Used when Bukkit API seeds initial player spawns (with no real entityId),
     * and later SPAWN_ENTITY packets arrive with the real entityId.
     * Removes the old entityId entry and creates a new TrackedEntity with the real entityId.
     */
    public void updateEntityId(UUID viewerUuid, UUID entityUuid, int newEntityId) {
        Map<UUID, TrackedEntity> byUuid = perPlayerByUuid.get(viewerUuid);
        if (byUuid == null) return;
        TrackedEntity existing = byUuid.get(entityUuid);
        if (existing == null) return;

        // Remove old entityId mapping
        Map<Integer, TrackedEntity> byId = perPlayerById.get(viewerUuid);
        if (byId != null) {
            byId.remove(existing.getEntityId());
        }

        // Create new TrackedEntity with the real entityId, preserving all other fields
        TrackedEntity updated = new TrackedEntity(newEntityId, existing.getUuid(), existing.isPlayer(),
                existing.getEntityTypeId(), existing.getX(), existing.getY(), existing.getZ(),
                existing.getYaw(), existing.getPitch());
        updated.setPlayerName(existing.getPlayerName());

        // Put into both maps
        if (byId != null) {
            byId.put(newEntityId, updated);
        }
        byUuid.put(entityUuid, updated);
    }

    public void removeEntity(UUID viewerUuid, int entityId) {
        Map<Integer, TrackedEntity> byId = perPlayerById.get(viewerUuid);
        if (byId == null) return;
        TrackedEntity entity = byId.remove(entityId);
        if (entity != null && entity.getUuid() != null) {
            Map<UUID, TrackedEntity> byUuid = perPlayerByUuid.get(viewerUuid);
            if (byUuid != null) {
                byUuid.remove(entity.getUuid());
            }
        }
    }

    public TrackedEntity getByEntityId(UUID viewerUuid, int entityId) {
        Map<Integer, TrackedEntity> byId = perPlayerById.get(viewerUuid);
        return byId != null ? byId.get(entityId) : null;
    }

    public TrackedEntity getByUuid(UUID viewerUuid, UUID uuid) {
        Map<UUID, TrackedEntity> byUuid = perPlayerByUuid.get(viewerUuid);
        return byUuid != null ? byUuid.get(uuid) : null;
    }

    public boolean isPlayer(UUID viewerUuid, int entityId) {
        TrackedEntity entity = getByEntityId(viewerUuid, entityId);
        return entity != null && entity.isPlayer();
    }

    /**
     * Returns all player entities currently visible to the given viewer.
     */
    public List<TrackedEntity> getVisiblePlayers(UUID viewerUuid) {
        List<TrackedEntity> players = new ArrayList<>();
        Map<Integer, TrackedEntity> byId = perPlayerById.get(viewerUuid);
        if (byId != null) {
            for (TrackedEntity entity : byId.values()) {
                if (entity.isPlayer()) {
                    players.add(entity);
                }
            }
        }
        return players;
    }

    /**
     * Find all viewer UUIDs whose tracking maps contain the given player UUID.
     * Used to route a player's C2S packets to the recordings tracking them.
     */
    public List<UUID> findViewersTracking(UUID playerUuid) {
        List<UUID> viewers = new ArrayList<>();
        for (Map.Entry<UUID, Map<UUID, TrackedEntity>> entry : perPlayerByUuid.entrySet()) {
            TrackedEntity entity = entry.getValue().get(playerUuid);
            if (entity != null && entity.isPlayer()) {
                viewers.add(entry.getKey());
            }
        }
        return viewers;
    }

    /**
     * Remove per-player tracking maps (on disconnect). Does NOT clear playerNames.
     */
    public void clearPlayer(UUID viewerUuid) {
        perPlayerById.remove(viewerUuid);
        perPlayerByUuid.remove(viewerUuid);
    }

    public void clear() {
        perPlayerById.clear();
        perPlayerByUuid.clear();
        playerNames.clear();
    }

    @Getter
    public static class TrackedEntity {
        private final int entityId;
        private final UUID uuid;
        private final boolean player;
        private final int entityTypeId;
        private volatile double x, y, z;
        private volatile float yaw, pitch;
        private volatile String playerName;

        public TrackedEntity(int entityId, UUID uuid, boolean player, int entityTypeId,
                             double x, double y, double z, float yaw, float pitch) {
            this.entityId = entityId;
            this.uuid = uuid;
            this.player = player;
            this.entityTypeId = entityTypeId;
            this.x = x;
            this.y = y;
            this.z = z;
            this.yaw = yaw;
            this.pitch = pitch;
        }

        public void setX(double x) { this.x = x; }
        public void setY(double y) { this.y = y; }
        public void setZ(double z) { this.z = z; }
        public void setYaw(float yaw) { this.yaw = yaw; }
        public void setPitch(float pitch) { this.pitch = pitch; }
        public void setPlayerName(String playerName) { this.playerName = playerName; }
        public int getTypeId() { return entityTypeId; }
    }
}
