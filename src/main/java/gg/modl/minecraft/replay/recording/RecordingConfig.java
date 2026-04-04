package gg.modl.minecraft.replay.recording;

/**
 * Configuration interface for replay recording consumers.
 * Both replay-lite-plugin and modl-bridge implement this from their own config files.
 */
public interface RecordingConfig {
    int bufferDurationSeconds();
    int maxDurationSeconds();
    int radiusBlocks();
    int moveThrottleMs();
    String uploadEndpoint();
    String uploadApiKey();
    String viewerBaseUrl();
    /** Minecraft version string (e.g. "1.21.4") for the replay header. */
    String mcVersion();
}
