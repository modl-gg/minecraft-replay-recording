package gg.modl.minecraft.replay.recording;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SpawnEntityClassifierTest {

    @Test
    void treatsKnownPlayerUuidAsPlayerWhenEntityTypeIsMissing() {
        assertEquals(
                SpawnEntityClassifier.SpawnKind.PLAYER,
                SpawnEntityClassifier.classify(true, null)
        );
    }

    @Test
    void skipsUnknownEntityWhenEntityTypeCannotBeResolved() {
        assertEquals(
                SpawnEntityClassifier.SpawnKind.SKIP_UNKNOWN,
                SpawnEntityClassifier.classify(false, null)
        );
    }
}
