package com.rutvapatel.pulsedrop.api;

import com.rutvapatel.pulsedrop.config.AppConfig;
import com.rutvapatel.pulsedrop.serde.JsonSupport;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public final class AnalyticsApi {
    private AnalyticsApi() {}

    public static void main(String[] args) throws Exception {
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        AppConfig config = AppConfig.fromEnvironment();
        AnalyticsRepository repository = new AnalyticsRepository(config);
        HttpServer server = HttpServer.create(new InetSocketAddress(config.apiPort()), 0);
        server.createContext("/health", exchange -> {
            if (!methodAllowed(exchange, "GET")) return;
            boolean healthy = repository.healthy();
            sendJson(exchange, healthy ? 200 : 503, Map.of(
                    "status", healthy ? "healthy" : "unhealthy",
                    "checkedAt", Instant.now()));
        });
        server.createContext("/api/v1/overview", apiHandler(repository::overview));
        server.createContext("/api/v1/content", apiHandler(repository::topContent));
        server.createContext("/api/v1/timeseries", apiHandler(repository::timeSeries));
        server.createContext("/", new DashboardHandler());
        server.setExecutor(Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors())));
        server.start();
        System.out.printf("PulseDrop analytics API listening on http://0.0.0.0:%d%n", config.apiPort());
    }

    private static HttpHandler apiHandler(SqlSupplier<?> supplier) {
        return exchange -> {
            if (!methodAllowed(exchange, "GET")) return;
            try {
                sendJson(exchange, 200, supplier.get());
            } catch (SQLException exception) {
                Map<String, Object> error = new LinkedHashMap<>();
                error.put("error", "analytics_query_failed");
                error.put("message", "The analytics store could not complete the request.");
                error.put("timestamp", Instant.now());
                sendJson(exchange, 503, error);
                System.err.println("Analytics query failed: " + exception.getMessage());
            }
        };
    }

    private static boolean methodAllowed(HttpExchange exchange, String expected) throws IOException {
        setCommonHeaders(exchange);
        if ("OPTIONS".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
            return false;
        }
        if (!expected.equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "method_not_allowed"));
            return false;
        }
        return true;
    }

    private static void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] payload = JsonSupport.MAPPER.writeValueAsBytes(body);
        setCommonHeaders(exchange);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, payload.length);
        exchange.getResponseBody().write(payload);
        exchange.close();
    }

    private static void setCommonHeaders(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
        exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        exchange.getResponseHeaders().set("X-Frame-Options", "DENY");
    }

    @FunctionalInterface
    private interface SqlSupplier<T> {
        T get() throws SQLException;
    }

    private static final class DashboardHandler implements HttpHandler {
        private static final Map<String, String> CONTENT_TYPES = Map.of(
                "index.html", "text/html; charset=utf-8",
                "styles.css", "text/css; charset=utf-8",
                "app.js", "application/javascript; charset=utf-8");

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!methodAllowed(exchange, "GET")) return;
            String requested = exchange.getRequestURI().getPath();
            String resource = "/".equals(requested) ? "index.html" : requested.substring(1);
            if (!CONTENT_TYPES.containsKey(resource)) {
                sendJson(exchange, 404, Map.of("error", "not_found"));
                return;
            }
            try (InputStream stream = AnalyticsApi.class.getResourceAsStream("/dashboard/" + resource)) {
                if (stream == null) {
                    sendJson(exchange, 404, Map.of("error", "not_found"));
                    return;
                }
                byte[] payload = stream.readAllBytes();
                exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPES.get(resource));
                exchange.getResponseHeaders().set("Cache-Control", "public, max-age=300");
                exchange.sendResponseHeaders(200, payload.length);
                exchange.getResponseBody().write(payload);
                exchange.close();
            }
        }
    }
}
