package gg.modl.minecraft.replay.recording;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityTrackerTest {

    @Test
    void trackedEntityPreservesSpawnTimeNoThrottleClassification() {
        EntityTracker tracker = new EntityTracker();
        UUID viewerUuid = UUID.randomUUID();
        UUID entityUuid = UUID.randomUUID();

        tracker.trackEntity(viewerUuid, 42, entityUuid, false, 1, true,
                1.0, 2.0, 3.0, 4.0f, 5.0f);

        EntityTracker.TrackedEntity tracked = tracker.getByEntityId(viewerUuid, 42);

        assertTrue(tracked.isNoThrottleMovement());
    }
}
