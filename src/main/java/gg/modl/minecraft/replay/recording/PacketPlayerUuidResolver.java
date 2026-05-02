package gg.modl.minecraft.replay.recording;

import java.lang.reflect.Method;
import java.util.UUID;

final class PacketPlayerUuidResolver {

    private PacketPlayerUuidResolver() {
    }

    static UUID resolve(UUID userUuid, Object player) {
        return resolve(userUuid, player, PacketPlayerUuidResolver::resolveNativePlayerUuid);
    }

    static UUID resolve(UUID userUuid, Object player, NativeResolver nativeResolver) {
        if (player != null && nativeResolver != null) {
            UUID nativeUuid = resolveNativePlayerUuid(player, nativeResolver);
            if (nativeUuid != null) {
                return nativeUuid;
            }
        }
        return userUuid;
    }

    private static UUID resolveNativePlayerUuid(Object player, NativeResolver nativeResolver) {
        try {
            return nativeResolver.resolve(player);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static UUID resolveNativePlayerUuid(Object player) {
        UUID uuid = invokeUuidGetter(player, "getUuid");
        if (uuid != null) {
            return uuid;
        }
        return invokeUuidGetter(player, "getUUID");
    }

    private static UUID invokeUuidGetter(Object player, String methodName) {
        try {
            Method method = player.getClass().getMethod(methodName);
            Object value = method.invoke(player);
            return value instanceof UUID ? (UUID) value : null;
        } catch (ReflectiveOperationException | SecurityException e) {
            return null;
        }
    }

    @FunctionalInterface
    interface NativeResolver {
        UUID resolve(Object player);
    }
}
