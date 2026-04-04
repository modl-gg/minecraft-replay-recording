package gg.modl.minecraft.replay.recording.upload;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import gg.modl.minecraft.replay.recording.RecordingConfig;
import lombok.Getter;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class ReplayUploadClient {

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 30_000;
    private static final int UPLOAD_READ_TIMEOUT_MS = 5 * 60 * 1000;

    private final RecordingConfig config;
    private final Logger logger;
    private final Gson gson;

    public ReplayUploadClient(RecordingConfig config, Logger logger) {
        this.config = config;
        this.logger = logger;
        this.gson = new Gson();
    }

    public CompletableFuture<UploadResult> uploadAsync(File replayFile, String mcVersion,
                                                        String targetUuid, String targetName) {
        String apiKey = config.uploadApiKey();
        if (apiKey == null || apiKey.isEmpty() || "CHANGE_ME".equals(apiKey)) {
            CompletableFuture<UploadResult> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("Upload API key not configured"));
            return f;
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                InitResponse initResponse = initUpload(replayFile, mcVersion, apiKey);
                uploadToStorage(replayFile, initResponse.uploadUrl);
                confirmUpload(initResponse.replayId, apiKey);
                return new UploadResult(initResponse.replayId, initResponse.viewerUrl);
            } catch (Exception e) {
                throw new RuntimeException("Upload failed: " + e.getMessage(), e);
            }
        });
    }

    private InitResponse initUpload(File file, String mcVersion, String apiKey) throws Exception {
        String signature = HmacSigner.sign(
                file.getName() + ":" + file.length(),
                apiKey
        );

        JsonObject body = new JsonObject();
        body.addProperty("mcVersion", mcVersion);
        body.addProperty("fileSize", file.length());
        body.addProperty("signature", signature);

        HttpURLConnection conn = (HttpURLConnection) new URL(config.uploadEndpoint() + "/v1/plugin/replay/upload").openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("X-API-Key", apiKey);
        conn.setDoOutput(true);

        byte[] bodyBytes = gson.toJson(body).getBytes(StandardCharsets.UTF_8);
        conn.getOutputStream().write(bodyBytes);
        conn.getOutputStream().close();

        int status = conn.getResponseCode();
        String responseBody = readResponseBody(conn);

        if (status != 200) {
            throw new RuntimeException("Init upload failed (HTTP " + status + "): " + responseBody);
        }

        JsonObject json = gson.fromJson(responseBody, JsonObject.class);
        if (json == null || !json.has("replayId") || !json.has("uploadUrl") || !json.has("viewerUrl")) {
            throw new RuntimeException("Malformed init response: " + responseBody);
        }
        return new InitResponse(
                json.get("replayId").getAsString(),
                json.get("uploadUrl").getAsString(),
                json.get("viewerUrl").getAsString()
        );
    }

    private void uploadToStorage(File file, String presignedUrl) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(presignedUrl).openConnection();
        conn.setRequestMethod("PUT");
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(UPLOAD_READ_TIMEOUT_MS);
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.setDoOutput(true);

        byte[] fileBytes = Files.readAllBytes(file.toPath());
        conn.getOutputStream().write(fileBytes);
        conn.getOutputStream().close();

        int status = conn.getResponseCode();
        if (status < 200 || status >= 300) {
            String responseBody = readResponseBody(conn);
            throw new RuntimeException("Storage upload failed (HTTP " + status + "): " + responseBody);
        }
    }

    private void confirmUpload(String replayId, String apiKey) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(config.uploadEndpoint() + "/v1/plugin/replay/confirm/" + replayId).openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        conn.setRequestProperty("X-API-Key", apiKey);
        conn.setDoOutput(true);
        conn.getOutputStream().close();

        int status = conn.getResponseCode();
        if (status != 200) {
            String responseBody = readResponseBody(conn);
            throw new RuntimeException("Confirm upload failed (HTTP " + status
                    + ") for replay " + replayId + ": " + responseBody);
        }
    }

    private static String readResponseBody(HttpURLConnection conn) {
        try {
            InputStream is = conn.getResponseCode() >= 400 ? conn.getErrorStream() : conn.getInputStream();
            if (is == null) return "";
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) sb.append(line);
                return sb.toString();
            }
        } catch (IOException e) {
            return "";
        }
    }

    @Getter
    public static class UploadResult {
        private final String replayId;
        private final String viewerUrl;

        public UploadResult(String replayId, String viewerUrl) {
            this.replayId = replayId;
            this.viewerUrl = viewerUrl;
        }
    }

    private static class InitResponse {
        final String replayId;
        final String uploadUrl;
        final String viewerUrl;

        InitResponse(String replayId, String uploadUrl, String viewerUrl) {
            this.replayId = replayId;
            this.uploadUrl = uploadUrl;
            this.viewerUrl = viewerUrl;
        }
    }
}
