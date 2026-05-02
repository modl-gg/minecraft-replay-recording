package gg.modl.minecraft.replay.recording;

final class PacketDecodeFailures {

    private static final String UNKNOWN_REGISTRY_PREFIX = "Can't resolve #";
    private static final String UNKNOWN_ENTITY_METADATA_TYPE_PREFIX = "Unknown entity metadata type id:";
    private static final String ITEM_REGISTRY_NAME = "'minecraft:item'";
    private static final String DATA_COMPONENT_TYPE_REGISTRY_NAME = "'minecraft:data_component_type'";

    private PacketDecodeFailures() {
    }

    static boolean isUnsupportedItemComponentDecodeFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null
                    && message.contains(UNKNOWN_REGISTRY_PREFIX)
                    && (message.contains(ITEM_REGISTRY_NAME)
                    || message.contains(DATA_COMPONENT_TYPE_REGISTRY_NAME))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    static boolean isUnsupportedEntityMetadataDecodeFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains(UNKNOWN_ENTITY_METADATA_TYPE_PREFIX)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
