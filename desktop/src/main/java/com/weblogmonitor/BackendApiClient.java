package com.weblogmonitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public final class BackendApiClient {
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(4))
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    private final ObjectMapper mapper = new ObjectMapper();
    private final String baseUrl = System.getenv().getOrDefault("BACKEND_URL", "http://127.0.0.1:8000");

    public JsonNode get(String path) throws IOException, InterruptedException {
        return sendJson(HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(20)).GET().build());
    }

    public JsonNode post(String path, JsonNode body) throws IOException, InterruptedException {
        return sendJson(HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : body.toString()))
                .build());
    }

    public JsonNode put(String path, JsonNode body) throws IOException, InterruptedException {
        return sendJson(HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body == null ? "" : body.toString()))
                .build());
    }

    public String getText(String path) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(30)).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        ensureSuccess(response);
        return response.body();
    }

    private JsonNode sendJson(HttpRequest request) throws IOException, InterruptedException {
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        ensureSuccess(response);
        return mapper.readTree(response.body());
    }

    private static void ensureSuccess(HttpResponse<String> response) throws IOException {
        if (response.statusCode() >= 400) {
            throw new IOException("Backend returned HTTP " + response.statusCode() + ": " + response.body());
        }
    }
}
