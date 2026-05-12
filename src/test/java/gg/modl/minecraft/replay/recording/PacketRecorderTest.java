package gg.modl.minecraft.replay.recording;

import com.github.retrooper.packetevents.event.PacketListenerCommon;
import com.github.retrooper.packetevents.event.ProtocolPacketEvent;
import com.github.retrooper.packetevents.manager.server.ServerVersion;
import com.github.retrooper.packetevents.protocol.ConnectionState;
import com.github.retrooper.packetevents.protocol.packettype.PacketTypeCommon;
import com.github.retrooper.packetevents.protocol.player.ClientVersion;
import com.github.retrooper.packetevents.protocol.player.User;
import com.github.retrooper.packetevents.wrapper.PacketWrapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PacketRecorderTest {

    @TempDir
    File tempDir;

    @Test
    void recognizesUnknownItemRegistryDecodeFailure() {
        IllegalStateException failure = new IllegalStateException(
                "Can't resolve #14265 (V_1_21) in 'minecraft:item'"
        );

        assertTrue(PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(failure));
    }

    @Test
    void recognizesWrappedUnknownDataComponentRegistryDecodeFailure() {
        RuntimeException failure = new RuntimeException(
                "Wrapper failed",
                new IllegalArgumentException("Can't resolve #225 (V_1_21) in 'minecraft:data_component_type'")
        );

        assertTrue(PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(failure));
    }

    @Test
    void ignoresUnrelatedRegistryDecodeFailure() {
        IllegalStateException failure = new IllegalStateException(
                "Can't resolve #12 (V_1_21) in 'minecraft:entity_type'"
        );

        assertFalse(PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(failure));
    }

    @Test
    void ignoresUnrelatedException() {
        assertFalse(PacketDecodeFailures.isUnsupportedItemComponentDecodeFailure(new RuntimeException("boom")));
    }

    @Test
    void recognizesUnknownEntityMetadataTypeDecodeFailure() {
        IllegalStateException failure = new IllegalStateException(
                "Unknown entity metadata type id: 33 version V_1_21"
        );

        assertTrue(PacketDecodeFailures.isUnsupportedEntityMetadataDecodeFailure(failure));
    }

    @Test
    void recognizesWrappedUnknownEntityMetadataTypeDecodeFailure() {
        RuntimeException failure = new RuntimeException(
                "Wrapper failed",
                new IllegalStateException("Unknown entity metadata type id: 32 version V_1_21")
        );

        assertTrue(PacketDecodeFailures.isUnsupportedEntityMetadataDecodeFailure(failure));
    }

    @Test
    void ignoresUnrelatedEntityMetadataException() {
        assertFalse(PacketDecodeFailures.isUnsupportedEntityMetadataDecodeFailure(
                new RuntimeException("Unknown entity type id: 33 version V_1_21")));
    }

    @Test
    void resolvesUuidFromPlatformPlayerWhenPacketEventsUserUuidIsMissing() {
        UUID playerUuid = UUID.randomUUID();
        Object nativePlayer = new Object();

        UUID resolved = PacketPlayerUuidResolver.resolve(
                null, nativePlayer, player -> player == nativePlayer ? playerUuid : null);

        assertEquals(playerUuid, resolved);
    }

    @Test
    void prefersNativePlayerUuidOverPacketEventsUserUuid() {
        UUID userUuid = UUID.randomUUID();
        UUID nativeUuid = UUID.randomUUID();

        UUID resolved = PacketPlayerUuidResolver.resolve(
                userUuid, new Object(), player -> nativeUuid);

        assertEquals(nativeUuid, resolved);
    }

    @Test
    void fallsBackToPacketEventsUserUuidWhenNativePlayerIsUnavailable() {
        UUID userUuid = UUID.randomUUID();

        UUID resolved = PacketPlayerUuidResolver.resolve(userUuid, null, null);

        assertEquals(userUuid, resolved);
    }

    @Test
    void ignoresPlatformPlayerResolverFailures() {
        UUID resolved = PacketPlayerUuidResolver.resolve(
                null, new Object(), player -> {
                    throw new IllegalStateException("wrong player type");
                });

        assertNull(resolved);
    }

    @Test
    void resolvesFabricStyleGetUuidNativePlayer() {
        UUID playerUuid = UUID.randomUUID();

        UUID resolved = PacketPlayerUuidResolver.resolve(null, new FabricStylePlayer(playerUuid));

        assertEquals(playerUuid, resolved);
    }

    @Test
    void resolvesMojangStyleGetUUIDNativePlayer() {
        UUID playerUuid = UUID.randomUUID();

        UUID resolved = PacketPlayerUuidResolver.resolve(null, new MojangStylePlayer(playerUuid));

        assertEquals(playerUuid, resolved);
    }

    @Test
    void readOnlyObservationRestoresPacketEventReEncodeState() throws Exception {
        TestProtocolPacketEvent event = allocateTestProtocolPacketEvent();
        PacketWrapper<?> previousWrapper = PacketWrapper.createDummyWrapper(ClientVersion.V_1_21_7);
        event.setLastUsedWrapper(previousWrapper);
        event.markForReEncode(true);
        PacketWrapper<?> observedWrapper = PacketWrapper.createDummyWrapper(ClientVersion.V_1_21_7);

        ReadOnlyPacketEventScope.run(event, () -> {
            event.setLastUsedWrapper(observedWrapper);
            event.markForReEncode(false);
        });

        assertSame(previousWrapper, event.getLastUsedWrapper());
        assertTrue(event.needsReEncode());
    }

    @Test
    void readOnlyObservationRestoresPacketEventStateAfterFailure() throws Exception {
        TestProtocolPacketEvent event = allocateTestProtocolPacketEvent();
        event.markForReEncode(false);

        assertThrows(IllegalStateException.class, () ->
                ReadOnlyPacketEventScope.run(event, () -> {
                    event.setLastUsedWrapper(PacketWrapper.createDummyWrapper(ClientVersion.V_1_21_7));
                    event.markForReEncode(true);
                    throw new IllegalStateException("decode failed");
                }));

        assertNull(event.getLastUsedWrapper());
        assertFalse(event.needsReEncode());
    }

    @Test
    void skinDownloadExecutorIsLifecycleOwned() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingManager manager = new RecordingManager(new TestRecordingConfig(), tempDir, Logger.getAnonymousLogger());
        PacketRecorder recorder = new PacketRecorder(manager, new TestRecordingConfig(), Logger.getAnonymousLogger(), executor);

        recorder.shutdownSkinDownloadExecutor();

        assertTrue(executor.isShutdown());
    }

    public static final class FabricStylePlayer {
        private final UUID uuid;

        private FabricStylePlayer(UUID uuid) {
            this.uuid = uuid;
        }

        public UUID getUuid() {
            return uuid;
        }
    }

    public static final class MojangStylePlayer {
        private final UUID uuid;

        private MojangStylePlayer(UUID uuid) {
            this.uuid = uuid;
        }

        public UUID getUUID() {
            return uuid;
        }
    }

    private static TestProtocolPacketEvent allocateTestProtocolPacketEvent() throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);
        Method allocateInstance = unsafeClass.getMethod("allocateInstance", Class.class);
        return (TestProtocolPacketEvent) allocateInstance.invoke(unsafe, TestProtocolPacketEvent.class);
    }

    private static final class TestProtocolPacketEvent extends ProtocolPacketEvent {
        private TestProtocolPacketEvent() {
            super(0, (PacketTypeCommon) null, ServerVersion.V_1_21_8,
                    null, new User(null, ConnectionState.PLAY, ClientVersion.V_1_21_7, null), null, null);
        }

        @Override
        public void call(PacketListenerCommon listener) {
        }
    }

    private static final class TestRecordingConfig implements RecordingConfig {
        @Override
        public int bufferDurationSeconds() {
            return 30;
        }

        @Override
        public int maxDurationSeconds() {
            return 60;
        }

        @Override
        public int radiusBlocks() {
            return 16;
        }

        @Override
        public int moveThrottleMs() {
            return 50;
        }

        @Override
        public String uploadEndpoint() {
            return "https://example.com";
        }

        @Override
        public String uploadApiKey() {
            return "CHANGE_ME";
        }

        @Override
        public String viewerBaseUrl() {
            return "https://example.com/replay";
        }

        @Override
        public String mcVersion() {
            return "1.21.7";
        }
    }

}
