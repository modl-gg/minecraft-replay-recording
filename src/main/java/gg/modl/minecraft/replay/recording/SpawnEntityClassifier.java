package gg.modl.minecraft.replay.recording;

import com.github.retrooper.packetevents.protocol.entity.type.EntityType;
import com.github.retrooper.packetevents.protocol.entity.type.EntityTypes;

final class SpawnEntityClassifier {

    private SpawnEntityClassifier() {
    }

    static SpawnKind classify(boolean knownPlayer, EntityType entityType) {
        if (knownPlayer) {
            return SpawnKind.PLAYER;
        }
        if (entityType == null) {
            return SpawnKind.SKIP_UNKNOWN;
        }
        if (entityType == EntityTypes.PLAYER) {
            return SpawnKind.PLAYER;
        }
        return SpawnKind.NON_PLAYER;
    }

    enum SpawnKind {
        PLAYER,
        NON_PLAYER,
        SKIP_UNKNOWN
    }
}
