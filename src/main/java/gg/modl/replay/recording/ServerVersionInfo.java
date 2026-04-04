package gg.modl.replay.recording;

import com.github.retrooper.packetevents.protocol.player.ClientVersion;

/**
 * Parses a Minecraft version string and exposes world-height and block-format
 * information needed to correctly record replays across server versions.
 */
public final class ServerVersionInfo {

    private final int major;
    private final int minor;
    private final int patch;
    private final int minY;
    private final int maxY;
    private final int sectionsPerChunk;

    public ServerVersionInfo(String mcVersion) {
        String[] parts = mcVersion.split("\\.");
        this.major = parts.length > 0 ? parseIntSafe(parts[0]) : 1;
        this.minor = parts.length > 1 ? parseIntSafe(parts[1]) : 0;
        this.patch = parts.length > 2 ? parseIntSafe(parts[2]) : 0;

        // 1.18+ introduced extended world height (-64 to 320)
        if (major >= 2 || (major == 1 && minor >= 18)) {
            this.minY = -64;
            this.maxY = 320;
        } else {
            this.minY = 0;
            this.maxY = 256;
        }
        this.sectionsPerChunk = (maxY - minY) / 16;
    }

    /** True for pre-1.13 servers that use legacy block IDs (blockId << 4 | data). */
    public boolean isLegacy() {
        return major == 1 && minor < 13;
    }

    /** Map this MC version to the closest PacketEvents ClientVersion. */
    public ClientVersion toClientVersion() {
        // Walk the ClientVersion enum and find the best match by protocol version.
        // ClientVersion names follow V_1_X or V_1_X_Y patterns.
        ClientVersion best = null;
        for (ClientVersion cv : ClientVersion.values()) {
            if (cv.isNewerThan(ClientVersion.V_1_7_10) || cv == ClientVersion.V_1_7_10) {
                String name = cv.name(); // e.g. "V_1_8", "V_1_8_1", "V_1_21_4"
                if (!name.startsWith("V_")) continue;
                int[] parsed = parseClientVersionName(name);
                if (parsed == null) continue;
                if (parsed[0] == major && parsed[1] == minor) {
                    // Same major.minor — pick closest patch
                    if (best == null) {
                        best = cv;
                    } else {
                        int[] bestParsed = parseClientVersionName(best.name());
                        if (bestParsed != null && Math.abs(parsed[2] - patch) < Math.abs(bestParsed[2] - patch)) {
                            best = cv;
                        }
                    }
                }
            }
        }
        return best != null ? best : ClientVersion.getLatest();
    }

    private static int[] parseClientVersionName(String name) {
        // "V_1_8" -> [1, 8, 0], "V_1_21_4" -> [1, 21, 4]
        String[] parts = name.substring(2).split("_"); // skip "V_"
        if (parts.length < 2) return null;
        try {
            int ma = Integer.parseInt(parts[0]);
            int mi = Integer.parseInt(parts[1]);
            int pa = parts.length > 2 ? Integer.parseInt(parts[2]) : 0;
            return new int[]{ma, mi, pa};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int parseIntSafe(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public int getMinY() { return minY; }
    public int getMaxY() { return maxY; }
    public int getSectionsPerChunk() { return sectionsPerChunk; }
    public int getMajor() { return major; }
    public int getMinor() { return minor; }
    public int getPatch() { return patch; }
}
