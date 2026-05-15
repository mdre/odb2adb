package com.migration;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;

public class ArcadeBatchClient {
    private final HttpClient httpClient;
    private final String url;
    private final String authHeader;
    private final Gson gson;
    private boolean debug = false;

    public ArcadeBatchClient(String host, int port, String database, String user, String password) {
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(30))
                .build();
        this.url = String.format("http://%s:%d/api/v1/batch/%s", host, port, database);
        this.authHeader = "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes());
        this.gson = new Gson();
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    private void logError(HttpResponse<String> response) {
        synchronized (this) {
            try (FileWriter fw = new FileWriter("responseError.log", true);
                    PrintWriter pw = new PrintWriter(fw)) {
                pw.println(response.statusCode() + ": " + response.body());
                pw.println("-");
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Sends a JSONL payload to ArcadeDB GraphBatch endpoint
     * 
     * @param jsonlPayload The multiline JSONL string
     * @param lightEdges   true if we want optimized edges without properties
     * @return The parsed JSON response containing ID mappings
     */
    public JsonObject sendBatch(String jsonlPayload, boolean lightEdges) throws Exception {
        if (debug) {
            synchronized (this) {
                try (FileWriter fw = new FileWriter("debug.log", true);
                        PrintWriter pw = new PrintWriter(fw)) {
                    pw.println(jsonlPayload);
                } catch (IOException ignored) {
                }
            }
        }
        String endpoint = url + (lightEdges ? "?lightEdges=false" : "");

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .header("Content-Type", "application/x-ndjson")
                .header("Authorization", authHeader)
                .POST(HttpRequest.BodyPublishers.ofString(jsonlPayload))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200 && response.statusCode() != 201) {
            logError(response);
            throw new RuntimeException("Error from ArcadeDB HTTP " + response.statusCode() + ": " + response.body());
        }

        return gson.fromJson(response.body(), JsonObject.class);
    }

    /**
     * Executes a command in ArcadeDB using the specified language
     * 
     * @param language The language of the command (e.g., sql, sqlscript)
     * @param command  The command to execute
     */
    public void executeCommand(String language, String command) throws Exception {
        if (debug) {
            synchronized (this) {
                try (FileWriter fw = new FileWriter("debug.log", true);
                        PrintWriter pw = new PrintWriter(fw)) {
                    pw.println(command);
                } catch (IOException ignored) {
                }
            }
        }
        String commandUrl = url.replace("/batch/", "/command/");
        String payload = gson.toJson(Map.of("language", language, "command", command));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(commandUrl))
                .header("Content-Type", "application/json")
                .header("Authorization", authHeader)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200 && response.statusCode() != 201) {
            // Ignore if it already exists to allow multiple runs
            if (!response.body().contains("already exists")) {
                logError(response);
                throw new RuntimeException("Error executing command [" + command + "]: " + response.body());
            }
        }
    }

    /**
     * Executes a SQL command in ArcadeDB
     * 
     * @param sqlCommand The command to execute
     */
    public void executeCommand(String sqlCommand) throws Exception {
        executeCommand("sql", sqlCommand);
    }

    public JsonObject executeQuery(String sqlCommand) throws Exception {
        String queryUrl = url.replace("/batch/", "/query/");
        String payload = gson.toJson(Map.of("language", "sql", "command", sqlCommand));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(queryUrl))
                .header("Content-Type", "application/json")
                .header("Authorization", authHeader)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200 && response.statusCode() != 201) {
            logError(response);
            throw new RuntimeException("Error executing query [" + sqlCommand + "]: " + response.body());
        }

        return gson.fromJson(response.body(), JsonObject.class);
    }
}
