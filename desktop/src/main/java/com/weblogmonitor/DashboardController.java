package com.weblogmonitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.scene.Node;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.PieChart;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class DashboardController {
    @FXML private Label pageTitle;
    @FXML private Label connectionStatus;
    @FXML private Label lastUpdated;
    @FXML private Label backendServiceStatus;
    @FXML private Label datasetServiceStatus;
    @FXML private Label kafkaServiceStatus;
    @FXML private Label sparkServiceStatus;
    @FXML private Label mysqlServiceStatus;
    @FXML private Label totalRequests;
    @FXML private Label requestsPerSecond;
    @FXML private Label successRequests;
    @FXML private Label clientErrors;
    @FXML private Label serverErrors;
    @FXML private Label activeAlerts;
    @FXML private TableView<JsonNode> logTable;
    @FXML private TableColumn<JsonNode, String> timeColumn;
    @FXML private TableColumn<JsonNode, String> ipColumn;
    @FXML private TableColumn<JsonNode, String> methodColumn;
    @FXML private TableColumn<JsonNode, String> urlColumn;
    @FXML private TableColumn<JsonNode, String> statusColumn;
    @FXML private TableColumn<JsonNode, String> bytesColumn;
    @FXML private VBox content;

    private final BackendApiClient api = new BackendApiClient();
    private final ObjectMapper mapper = new ObjectMapper();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "dashboard-refresh");
        thread.setDaemon(true);
        return thread;
    });
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean liveRequestInFlight = new AtomicBoolean();
    private final List<Node> dashboardContent = new ArrayList<>();
    private String currentPage = "Dashboard";
    private int pageGeneration;
    private int logOffset;
    private int logTotal;
    private long latestStreamId;
    private String alertSeverity = "ALL";
    private String alertStatus = "ALL";

    @FXML
    private void initialize() {
        dashboardContent.addAll(content.getChildren());
        configureLogTable(logTable);
        showDashboard();
        refreshServiceStatus();
        scheduler.scheduleAtFixedRate(this::pollLiveData, 1, 2, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::pollServiceStatus, 10, 15, TimeUnit.SECONDS);
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }

    @FXML private void showDashboard() {
        setPage("Dashboard");
        content.getChildren().setAll(dashboardContent);
        connectionStatus.setText("Loading historical dataset aggregates");
        loadDashboard();
        loadDashboardLogs();
    }

    @FXML private void showLiveMonitoring() {
        setPage("Live Monitoring");
        content.getChildren().setAll(buildLiveMonitoring());
        connectionStatus.setText("Connecting to historical replay stream");
        requestLiveData();
    }

    @FXML private void showAnalytics() {
        setPage("Analytics");
        content.getChildren().setAll(buildAnalytics());
    }

    @FXML private void showLogExplorer() {
        setPage("Log Explorer");
        content.getChildren().setAll(buildLogExplorer());
        searchLogs();
    }

    @FXML private void showAlerts() {
        setPage("Alerts");
        content.getChildren().setAll(buildAlerts());
        loadAlerts();
    }

    @FXML private void showSystemStatus() {
        setPage("System Status");
        content.getChildren().setAll(buildSystemStatus());
        loadSystemStatus();
    }

    @FXML private void refresh() {
        switch (currentPage) {
            case "Dashboard" -> {
                loadDashboard();
                loadDashboardLogs();
            }
            case "Live Monitoring" -> requestLiveData();
            case "Analytics" -> loadAnalytics();
            case "Log Explorer" -> searchLogs();
            case "Alerts" -> loadAlerts();
            case "System Status" -> loadSystemStatus();
            default -> showDashboard();
        }
    }

    private void setPage(String name) {
        currentPage = name;
        pageGeneration++;
        pageTitle.setText(name);
    }

    private void loadDashboard() {
        runRequest("/api/dashboard", json -> {
            totalRequests.setText(formatNumber(json.path("total_requests").asLong()));
            successRequests.setText(formatNumber(json.path("successful_requests").asLong()));
            clientErrors.setText(formatNumber(json.path("client_errors").asLong()));
            serverErrors.setText(formatNumber(json.path("server_errors").asLong()));
            activeAlerts.setText(formatNumber(json.path("active_alerts").asLong()));
            requestsPerSecond.setText(String.format("%.2f", json.path("requests_per_second").asDouble()));
            connectionStatus.setText("● Historical dataset connected");
            connectionStatus.getStyleClass().setAll("status-online");
            updateTime();
        });
        runRequest("/api/analytics/traffic", this::updateDashboardTraffic);
    }

    private void updateDashboardTraffic(JsonNode rows) {
        LineChart<String, Number> chart = createTrafficChart();
        chart.setPrefHeight(230);
        LineChart.Series<String, Number> series = new LineChart.Series<>();
        series.setName("Requests per minute");
        rows.forEach(row -> series.getData().add(
            new LineChart.Data<>(row.path("timestamp").asText(""), row.path("request_count").asLong())
        ));
        chart.getData().setAll(series);
        if (dashboardContent.stream().noneMatch(node -> "dashboardTrafficChart".equals(node.getId()))) {
            chart.setId("dashboardTrafficChart");
            int insertAt = Math.max(0, dashboardContent.size() - 1);
            dashboardContent.add(insertAt, chart);
            content.getChildren().setAll(dashboardContent);
        }
    }

    private void loadDashboardLogs() {
        runRequest("/api/logs?limit=100", json -> {
            logTotal = json.path("total").asInt();
            logTable.setItems(items(json.path("items")));
        });
    }

    private VBox buildLiveMonitoring() {
        Label description = new Label("Historical dataset replay (not live production traffic). Events are persisted in MySQL and published to Kafka when available.");
        description.getStyleClass().add("muted-text");
        Label telemetry = new Label("Waiting for stream telemetry");
        telemetry.setId("liveTelemetry");
        Button start = new Button("Resume replay");
        start.setOnAction(event -> postAction("/api/stream/start", null, this::requestLiveData));
        Button pause = new Button("Pause replay");
        pause.setOnAction(event -> postAction("/api/stream/pause", null, this::requestLiveData));
        HBox controls = new HBox(10, start, pause, telemetry);
        controls.setAlignment(javafx.geometry.Pos.CENTER_LEFT);
        TableView<JsonNode> liveTable = createLogsTable();
        liveTable.setId("liveLogTable");
        liveTable.setPrefHeight(580);
        VBox panel = panel("Stream events", description, liveTable);
        VBox.setVgrow(panel, Priority.ALWAYS);
        return new VBox(16, controls, panel);
    }

    private void pollLiveData() {
        if (!"Live Monitoring".equals(currentPage) || !liveRequestInFlight.compareAndSet(false, true)) {
            return;
        }
        CompletableFuture.supplyAsync(() -> {
            try {
                JsonNode status = api.get("/api/stream/status");
                JsonNode events = api.get("/api/stream/events?since_id=" + latestStreamId + "&limit=200");
                return new JsonNode[]{status, events};
            } catch (Exception error) {
                throw new RuntimeException(error);
            }
        }).whenComplete((response, error) -> Platform.runLater(() -> {
            liveRequestInFlight.set(false);
            if (!"Live Monitoring".equals(currentPage)) {
                return;
            }
            if (error != null) {
                showConnectionError(error);
                return;
            }
            updateLiveView(response[0], response[1]);
        }));
    }

    private void requestLiveData() {
        if (!liveRequestInFlight.compareAndSet(false, true)) {
            return;
        }
        CompletableFuture.supplyAsync(() -> {
            try {
                return new JsonNode[]{
                    api.get("/api/stream/status"),
                    api.get("/api/stream/events?since_id=" + latestStreamId + "&limit=200")
                };
            } catch (Exception error) {
                throw new RuntimeException(error);
            }
        }).whenComplete((response, error) -> Platform.runLater(() -> {
            liveRequestInFlight.set(false);
            if (!"Live Monitoring".equals(currentPage)) {
                return;
            }
            if (error != null) {
                showConnectionError(error);
            } else {
                updateLiveView(response[0], response[1]);
            }
        }));
    }

    private void updateLiveView(JsonNode status, JsonNode events) {
        Label telemetry = (Label) content.lookup("#liveTelemetry");
        TableView<JsonNode> table = (TableView<JsonNode>) content.lookup("#liveLogTable");
        if (telemetry == null || table == null) {
            return;
        }
        telemetry.setText(String.format(
            "Replay %s · %.1f events/s · %s persisted · %s",
            status.path("is_active").asBoolean() ? "running" : "paused",
            status.path("current_throughput_eps").asDouble(),
            formatNumber(status.path("persisted_events").asLong()),
            status.path("kafka_status").asText("Kafka unavailable")
        ));
        var merged = FXCollections.<JsonNode>observableArrayList(table.getItems());
        events.forEach(event -> {
            latestStreamId = Math.max(latestStreamId, event.path("stream_id").asLong());
            merged.add(0, event);
        });
        if (merged.size() > 200) {
            merged.remove(200, merged.size());
        }
        table.setItems(merged);
        connectionStatus.setText("● Historical replay connected");
        connectionStatus.getStyleClass().setAll("status-online");
        updateTime();
    }

    private VBox buildAnalytics() {
        PieChart statusChart = new PieChart();
        statusChart.setTitle("HTTP status distribution");
        statusChart.setId("analyticsStatusChart");
        LineChart<String, Number> trafficChart = createTrafficChart();
        trafficChart.setId("analyticsTrafficChart");
        trafficChart.setPrefHeight(330);
        BarChart<String, Number> endpoints = new BarChart<>(new CategoryAxis(), new NumberAxis());
        endpoints.setTitle("Top request paths");
        endpoints.setId("topEndpointsChart");
        endpoints.setPrefHeight(330);
        TableView<JsonNode> methodTable = createDataTable(
            new String[][]{{"method", "Method"}, {"request_count", "Requests"}, {"percentage", "Percent"}}
        );
        methodTable.setPrefHeight(220);
        VBox layout = new VBox(16, statusChart, trafficChart, endpoints, panel("HTTP method distribution", methodTable));
        runRequest("/api/analytics/status", rows -> {
            List<PieChart.Data> data = new ArrayList<>();
            rows.forEach(row -> data.add(
                new PieChart.Data(row.path("category").asText(), row.path("request_count").asLong())
            ));
            statusChart.setData(FXCollections.observableArrayList(data));
        });
        runRequest("/api/analytics/traffic", rows -> {
            LineChart.Series<String, Number> series = new LineChart.Series<>();
            series.setName("Requests per minute");
            rows.forEach(row -> series.getData().add(
                new LineChart.Data<>(row.path("timestamp").asText(""), row.path("request_count").asLong())
            ));
            trafficChart.getData().setAll(series);
        });
        runRequest("/api/analytics/top-urls?limit=10", rows -> {
            BarChart.Series<String, Number> series = new BarChart.Series<>();
            rows.forEach(row -> series.getData().add(
                new BarChart.Data<>(row.path("url").asText(), row.path("request_count").asLong())
            ));
            endpoints.getData().setAll(series);
        });
        runRequest("/api/analytics/methods", rows -> methodTable.setItems(items(rows)));
        runRequest("/api/analytics/bandwidth", data ->
            connectionStatus.setText("Dataset bandwidth: " + data.path("total_gigabytes").asText("n/a") + " GB")
        );
        runRequest("/api/analytics/latency", data ->
            lastUpdated.setText(data.path("detail").asText("Latency unavailable"))
        );
        return layout;
    }

    private void loadAnalytics() {
        content.getChildren().setAll(buildAnalytics());
    }

    private VBox buildLogExplorer() {
        TextField keyword = new TextField();
        keyword.setPromptText("URL or user agent");
        TextField ip = new TextField();
        ip.setPromptText("Client IP");
        TextField method = new TextField();
        method.setPromptText("Method");
        TextField status = new TextField();
        status.setPromptText("Status");
        ComboBox<String> source = new ComboBox<>(FXCollections.observableArrayList("all", "historical", "replay", "live"));
        source.setValue("all");
        Button search = new Button("Search");
        Label count = new Label("0 records");
        count.setId("logResultCount");
        Runnable executeSearch = () -> {
            logOffset = 0;
            searchLogs(keyword.getText(), ip.getText(), method.getText(), status.getText(), source.getValue(), count);
        };
        search.setOnAction(event -> executeSearch.run());
        HBox filters = new HBox(8, keyword, ip, method, status, source, search);
        HBox.setHgrow(keyword, Priority.ALWAYS);
        HBox.setHgrow(ip, Priority.ALWAYS);
        HBox.setHgrow(method, Priority.ALWAYS);
        HBox.setHgrow(status, Priority.ALWAYS);
        Button export = new Button("Export CSV");
        export.setOnAction(event -> exportLogs(keyword.getText(), ip.getText(), method.getText(), status.getText(), source.getValue()));
        TableView<JsonNode> table = createLogsTable();
        table.setId("explorerLogTable");
        table.setPrefHeight(560);
        Button previous = new Button("Previous");
        previous.setOnAction(event -> {
            logOffset = Math.max(0, logOffset - 100);
            searchLogs(keyword.getText(), ip.getText(), method.getText(), status.getText(), source.getValue(), count);
        });
        Button next = new Button("Next");
        next.setOnAction(event -> {
            if (logOffset + 100 < logTotal) {
                logOffset += 100;
                searchLogs(keyword.getText(), ip.getText(), method.getText(), status.getText(), source.getValue(), count);
            }
        });
        HBox paging = new HBox(10, previous, next, count, export);
        return new VBox(14, filters, table, paging);
    }

    private void searchLogs() {
        searchLogs("", "", "", "", "all", null);
    }

    private void searchLogs(String keyword, String ip, String method, String status, String source, Label count) {
        StringBuilder path = new StringBuilder("/api/logs/search?limit=100&offset=").append(logOffset);
        appendQuery(path, "keyword", keyword);
        appendQuery(path, "ip", ip);
        appendQuery(path, "method", method);
        appendQuery(path, "status", status);
        if (source != null && !source.equals("all")) {
            appendQuery(path, "source_type", source);
        }
        String endpoint = path.toString();
        int generation = pageGeneration;
        connectionStatus.setText("Searching indexed log records");
        runRequest(endpoint, json -> {
            if (generation != pageGeneration) {
                return;
            }
            logTotal = json.path("total").asInt();
            TableView<JsonNode> table = (TableView<JsonNode>) content.lookup("#explorerLogTable");
            if (table != null) {
                table.setItems(items(json.path("items")));
            } else if ("Dashboard".equals(currentPage)) {
                logTable.setItems(items(json.path("items")));
            }
            if (count != null) {
                count.setText(String.format("%,d matching · page %d", logTotal, logOffset / 100 + 1));
            } else {
                Label resultCount = (Label) content.lookup("#logResultCount");
                if (resultCount != null) {
                    resultCount.setText(String.format("%,d matching", logTotal));
                }
            }
            connectionStatus.setText("● Indexed log search connected");
            connectionStatus.getStyleClass().setAll("status-online");
        });
    }

    private void exportLogs(String keyword, String ip, String method, String status, String source) {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Export filtered logs");
        chooser.setInitialFileName("web_logs_export.csv");
        File output = chooser.showSaveDialog(content.getScene().getWindow());
        if (output == null) {
            return;
        }
        StringBuilder path = new StringBuilder("/api/logs/export?limit=10000");
        appendQuery(path, "keyword", keyword);
        appendQuery(path, "ip", ip);
        appendQuery(path, "method", method);
        appendQuery(path, "status", status);
        if (source != null && !source.equals("all")) {
            appendQuery(path, "source_type", source);
        }
        CompletableFuture.runAsync(() -> {
            try {
                java.nio.file.Files.writeString(output.toPath(), api.getText(path.toString()));
                Platform.runLater(() -> connectionStatus.setText("Exported " + output.getName()));
            } catch (Exception error) {
                Platform.runLater(() -> showConnectionError(error));
            }
        });
    }

    private VBox buildAlerts() {
        ComboBox<String> severity = new ComboBox<>(FXCollections.observableArrayList("ALL", "CRITICAL", "WARNING", "INFO"));
        severity.setValue(alertSeverity);
        severity.setOnAction(event -> {
            alertSeverity = severity.getValue();
            loadAlerts();
        });
        ComboBox<String> state = new ComboBox<>(FXCollections.observableArrayList("ALL", "OPEN", "ACKNOWLEDGED", "RESOLVED"));
        state.setValue(alertStatus);
        state.setOnAction(event -> {
            alertStatus = state.getValue();
            loadAlerts();
        });
        TableView<JsonNode> table = createDataTable(new String[][]{
            {"id", "ID"}, {"alert_type", "Type"}, {"severity", "Severity"},
            {"description", "Description"}, {"source", "Source"}, {"status", "Status"}, {"created_at", "Created"}
        });
        table.setId("alertsTable");
        table.setPrefHeight(560);
        Button acknowledge = new Button("Acknowledge");
        acknowledge.setOnAction(event -> updateSelectedAlert(table, "ACKNOWLEDGED"));
        Button resolve = new Button("Resolve");
        resolve.setOnAction(event -> updateSelectedAlert(table, "RESOLVED"));
        Button create = new Button("Create rule alert");
        create.setOnAction(event -> createManualAlert());
        return new VBox(14, new HBox(10, severity, state, acknowledge, resolve, create), table);
    }

    private void loadAlerts() {
        String endpoint = "/api/alerts?severity=" + encode(alertSeverity) + "&status=" + encode(alertStatus);
        runRequest(endpoint, rows -> {
            TableView<JsonNode> table = (TableView<JsonNode>) content.lookup("#alertsTable");
            if (table != null) {
                table.setItems(items(rows));
                connectionStatus.setText("● " + rows.size() + " alerts loaded");
                connectionStatus.getStyleClass().setAll("status-online");
            }
        });
    }

    private void updateSelectedAlert(TableView<JsonNode> table, String status) {
        JsonNode selected = table.getSelectionModel().getSelectedItem();
        if (selected == null) {
            connectionStatus.setText("Select an alert first");
            return;
        }
        try {
            putAction(
                "/api/alerts/" + selected.path("id").asLong() + "/status",
                mapper.createObjectNode().put("status", status),
                this::loadAlerts
            );
        } catch (Exception error) {
            showConnectionError(error);
        }
    }

    private void createManualAlert() {
        TextInputDialog dialog = new TextInputDialog();
        dialog.setTitle("Create alert");
        dialog.setHeaderText("Create a persisted rule alert");
        dialog.setContentText("Alert description:");
        dialog.showAndWait().filter(value -> !value.isBlank()).ifPresent(description -> {
            var request = mapper.createObjectNode()
                .put("alert_type", "MANUAL")
                .put("severity", "WARNING")
                .put("description", description)
                .put("source", "Desktop");
            postAction("/api/alerts", request, this::loadAlerts);
        });
    }

    private VBox buildSystemStatus() {
        TableView<JsonNode> table = createDataTable(new String[][]{
            {"service_name", "Service"}, {"status", "Status"}, {"details", "Details"},
            {"description", "Health details"}, {"last_checked_at", "Last checked"}
        });
        table.setId("systemStatusTable");
        table.setPrefHeight(560);
        Button check = new Button("Check services");
        check.setOnAction(event -> loadSystemStatus());
        return new VBox(14, new Label("Service status reflects backend health checks and processing telemetry."), check, table);
    }

    private void loadSystemStatus() {
        runRequest("/api/system/status", services -> {
            updateServiceStatus(services);
            TableView<JsonNode> table = (TableView<JsonNode>) content.lookup("#systemStatusTable");
            if (table != null) {
                table.setItems(items(services));
                long online = 0;
                for (JsonNode service : services) {
                    if ("ONLINE".equalsIgnoreCase(service.path("status").asText())) {
                        online++;
                    }
                }
                connectionStatus.setText("● " + online + " of " + services.size() + " services healthy");
                connectionStatus.getStyleClass().setAll(
                    online == services.size() ? "status-online" : "status-critical"
                );
            }
        });
    }

    private void refreshServiceStatus() {
        runGlobalRequest("/api/system/status", this::updateServiceStatus);
    }

    private void pollServiceStatus() {
        runGlobalRequest("/api/system/status", this::updateServiceStatus);
    }

    private void updateServiceStatus(JsonNode services) {
        for (JsonNode service : services) {
            String name = service.path("service_name").asText();
            Label label = switch (name) {
                case "Python Backend API" -> backendServiceStatus;
                case "Dataset Pipeline" -> datasetServiceStatus;
                case "Kafka" -> kafkaServiceStatus;
                case "Spark" -> sparkServiceStatus;
                case "MySQL" -> mysqlServiceStatus;
                default -> null;
            };
            if (label == null) {
                continue;
            }
            String status = service.path("status").asText("UNKNOWN");
            String displayName = switch (name) {
                case "Python Backend API" -> "Backend API";
                case "Dataset Pipeline" -> "Dataset pipeline";
                default -> name;
            };
            label.setText("●  " + displayName + " (" + status + ")");
            label.getStyleClass().setAll("ONLINE".equalsIgnoreCase(status) ? "status-online" :
                "STARTING".equalsIgnoreCase(status) || "NOT_CONFIGURED".equalsIgnoreCase(status)
                    ? "status-muted" : "status-critical");
        }
    }

    private TableView<JsonNode> createLogsTable() {
        TableView<JsonNode> table = new TableView<>();
        table.getColumns().setAll(
            jsonColumn("timestamp", "Timestamp", 180),
            jsonColumn("ip_address", "IP address", 120),
            jsonColumn("http_method", "Method", 80),
            jsonColumn("url", "URL", 420),
            jsonColumn("status_code", "Status", 75),
            jsonColumn("response_bytes", "Bytes", 90),
            jsonColumn("source_type", "Source", 90)
        );
        configureRowColors(table);
        return table;
    }

    private void configureLogTable(TableView<JsonNode> table) {
        timeColumn.setCellValueFactory(cell -> text(cell.getValue(), "timestamp"));
        ipColumn.setCellValueFactory(cell -> text(cell.getValue(), "ip_address"));
        methodColumn.setCellValueFactory(cell -> text(cell.getValue(), "http_method"));
        urlColumn.setCellValueFactory(cell -> text(cell.getValue(), "url"));
        statusColumn.setCellValueFactory(cell -> text(cell.getValue(), "status_code"));
        bytesColumn.setCellValueFactory(cell -> text(cell.getValue(), "response_bytes"));
        configureRowColors(table);
    }

    private void configureRowColors(TableView<JsonNode> table) {
        table.setRowFactory(view -> new TableRow<>() {
            @Override
            protected void updateItem(JsonNode item, boolean empty) {
                super.updateItem(item, empty);
                getStyleClass().removeAll("log-error", "log-warning");
                if (!empty && item != null) {
                    int code = item.path("status_code").asInt();
                    if (code >= 500) {
                        getStyleClass().add("log-error");
                    } else if (code >= 400) {
                        getStyleClass().add("log-warning");
                    }
                }
            }
        });
    }

    private TableView<JsonNode> createDataTable(String[][] fields) {
        TableView<JsonNode> table = new TableView<>();
        for (String[] field : fields) {
            table.getColumns().add(jsonColumn(field[0], field[1], 170));
        }
        return table;
    }

    private static TableColumn<JsonNode, String> jsonColumn(String key, String title, double width) {
        TableColumn<JsonNode, String> column = new TableColumn<>(title);
        column.setPrefWidth(width);
        column.setCellValueFactory(cell -> text(cell.getValue(), key));
        column.setCellFactory(view -> new TableCell<>() {
            @Override
            protected void updateItem(String value, boolean empty) {
                super.updateItem(value, empty);
                setText(empty ? null : value);
                setTooltip(!empty && value != null && value.length() > 50 ? new Tooltip(value) : null);
            }
        });
        return column;
    }

    private static javafx.beans.property.SimpleStringProperty text(JsonNode node, String key) {
        String value = node.path(key).isMissingNode() || node.path(key).isNull()
            ? "-" : node.path(key).asText();
        return new javafx.beans.property.SimpleStringProperty(value);
    }

    private LineChart<String, Number> createTrafficChart() {
        CategoryAxis x = new CategoryAxis();
        x.setLabel("Time");
        NumberAxis y = new NumberAxis();
        y.setLabel("Requests");
        LineChart<String, Number> chart = new LineChart<>(x, y);
        chart.setTitle("Historical traffic");
        chart.setAnimated(false);
        chart.setCreateSymbols(false);
        return chart;
    }

    private VBox panel(String title, Node... nodes) {
        Label heading = new Label(title);
        heading.getStyleClass().add("panel-title");
        VBox panel = new VBox(10);
        panel.getStyleClass().add("panel");
        panel.getChildren().add(heading);
        panel.getChildren().addAll(nodes);
        return panel;
    }

    private javafx.collections.ObservableList<JsonNode> items(JsonNode array) {
        var result = FXCollections.<JsonNode>observableArrayList();
        if (array.isArray()) {
            array.forEach(result::add);
        }
        return result;
    }

    private void runRequest(String path, Consumer<JsonNode> handler) {
        int generation = pageGeneration;
        if (!inFlight.add(path)) {
            return;
        }
        CompletableFuture.supplyAsync(() -> {
            try {
                return api.get(path);
            } catch (Exception error) {
                throw new RuntimeException(error);
            }
        }).whenComplete((result, error) -> Platform.runLater(() -> {
            inFlight.remove(path);
            if (generation != pageGeneration) {
                return;
            }
            if (error != null) {
                showConnectionError(error);
            } else {
                handler.accept(result);
            }
        }));
    }

    private void runGlobalRequest(String path, Consumer<JsonNode> handler) {
        CompletableFuture.supplyAsync(() -> {
            try {
                return api.get(path);
            } catch (Exception error) {
                throw new RuntimeException(error);
            }
        }).thenAccept(result -> Platform.runLater(() -> handler.accept(result)))
            .exceptionally(error -> null);
    }

    private void postAction(String path, JsonNode body, Runnable success) {
        CompletableFuture.supplyAsync(() -> {
            try {
                return api.post(path, body);
            } catch (Exception error) {
                throw new RuntimeException(error);
            }
        }).thenAccept(result -> Platform.runLater(success))
            .exceptionally(error -> {
                Platform.runLater(() -> showConnectionError(error));
                return null;
            });
    }

    private void putAction(String path, JsonNode body, Runnable success) {
        CompletableFuture.supplyAsync(() -> {
            try {
                return api.put(path, body);
            } catch (Exception error) {
                throw new RuntimeException(error);
            }
        }).thenAccept(result -> Platform.runLater(success))
            .exceptionally(error -> {
                Platform.runLater(() -> showConnectionError(error));
                return null;
            });
    }

    private void showConnectionError(Throwable error) {
        Throwable cause = error.getCause() == null ? error : error.getCause();
        connectionStatus.setText("● Connection error (showing last data): " + cause.getMessage());
        connectionStatus.getStyleClass().setAll("status-critical");
        lastUpdated.setText("Data may be stale");
    }

    private void updateTime() {
        lastUpdated.setText("Updated " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
    }

    private static String formatNumber(long value) {
        return String.format("%,d", value);
    }

    private static void appendQuery(StringBuilder path, String key, String value) {
        if (value != null && !value.isBlank()) {
            path.append('&').append(encode(key)).append('=').append(encode(value.trim()));
        }
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
