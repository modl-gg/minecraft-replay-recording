package gg.modl.minecraft.replay.recording;

import com.github.retrooper.packetevents.event.ProtocolPacketEvent;
import com.github.retrooper.packetevents.wrapper.PacketWrapper;

final class ReadOnlyPacketEventScope {

    private ReadOnlyPacketEventScope() {
    }

    static void run(ProtocolPacketEvent event, Operation operation) throws Exception {
        PacketWrapper<?> previousWrapper = event.getLastUsedWrapper();
        boolean previousReEncode = event.needsReEncode();
        try {
            operation.run();
        } finally {
            event.setLastUsedWrapper(previousWrapper);
            event.markForReEncode(previousReEncode);
        }
    }

    interface Operation {
        void run() throws Exception;
    }
}
