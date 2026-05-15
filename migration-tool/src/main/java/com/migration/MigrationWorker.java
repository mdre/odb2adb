package com.migration;

import com.migration.ui.MigrationScreen;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class MigrationWorker implements Runnable {
    private final String className;
    private final boolean isEdge;
    private final long totalRecords;
    private final ODatabasePool orientPool;
    private final ArcadeBatchClient arcadeClient;
    private final MigrationScreen screen;
    private final int workerIndex;
    private final Gson gson = new com.google.gson.GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    private final int batchSize;
    private final int retryCount;
    private final JsonObject config;

    public MigrationWorker(String className, boolean isEdge, long totalRecords,
            ODatabasePool orientPool, ArcadeBatchClient arcadeClient,
            MigrationScreen screen, int workerIndex, int batchSize, int retryCount, JsonObject config) {
        this.className = className;
        this.isEdge = isEdge;
        this.totalRecords = totalRecords;
        this.orientPool = orientPool;
        this.arcadeClient = arcadeClient;
        this.screen = screen;
        this.workerIndex = workerIndex;
        this.batchSize = batchSize;
        this.retryCount = retryCount;
        this.config = config;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        long processed = 0;

        if (screen != null) {
            screen.updateThread(workerIndex,
                    String.format(java.util.Locale.forLanguageTag("es-AR"), "Starting %s : %s (0 / %,d)",
                            isEdge ? "Edge" : "Vertex", className, totalRecords));
        }

        try (ODatabaseSession db = orientPool.acquire()) {
            db.activateOnCurrentThread();
            List<String> schemaProperties = new ArrayList<>();
            OClass oClass = db.getMetadata().getSchema().getClass(className);
            if (oClass != null) {
                for (OProperty prop : oClass.properties()) {
                    schemaProperties.add(prop.getName());
                }
            }

            String workingMode = config.has("WORKING_MODE") ? config.get("WORKING_MODE").getAsString() : "BATCH";

            if ("SINGLE".equalsIgnoreCase(workingMode)) {
                runSingleMode(threadName, db, schemaProperties);
            } else {
                StringBuilder jsonlBuffer = new StringBuilder();
                int currentBatchCount = 0;
                boolean errorOccurred = false;
                List<String> currentBatchRids = new ArrayList<>();

                for (com.orientechnologies.orient.core.record.OElement row : db.browseClass(className, false)) {
                    if (errorOccurred)
                        break;

                    try {
                        if (isEdge) {
                            if (row.isEdge()) {
                                if (processEdge(row.asEdge().get(), schemaProperties, jsonlBuffer)) {
                                    currentBatchCount++;
                                }
                            }
                        } else {
                            if (row.isVertex()) {
                                if (processVertexToJSONL(row.asVertex().get(), schemaProperties, jsonlBuffer)) {
                                    currentBatchCount++;
                                    currentBatchRids.add(row.getIdentity().toString());
                                }
                            }
                        }
                    } catch (Exception e) {
                        String rid = row.getIdentity() != null ? row.getIdentity().toString() : "UNKNOWN";
                        logError(className, rid, e.getMessage());
                    }

                    if (currentBatchCount >= batchSize) {
                        boolean success = false;
                        for (int attempt = 1; attempt <= retryCount; attempt++) {
                            try {
                                if (isEdge) {
                                    arcadeClient.executeCommand("sqlscript", jsonlBuffer.toString());
                                } else {
                                    arcadeClient.sendBatch(jsonlBuffer.toString(), false);
                                }
                                success = true;
                                break;
                            } catch (Exception e) {
                                if (attempt == retryCount) {
                                    try (FileWriter fw = new FileWriter("migration.log", true);
                                            PrintWriter pw = new PrintWriter(fw)) {
                                        pw.printf("[%s] Error sending batch for %s after %d attempts: %s%n", threadName,
                                                className, retryCount, e.getMessage());
                                    } catch (IOException ignored) {
                                    }
                                    errorOccurred = true;
                                } else {
                                    try {
                                        Thread.sleep(500 * attempt);
                                    } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                            }
                        }

                        if (success) {
                            if (!isEdge) {
                                for (String rid : currentBatchRids) {
                                    MapDBStore.addVertex(rid);
                                }
                            }
                            processed += currentBatchCount;
                            currentBatchCount = 0;
                            jsonlBuffer.setLength(0);
                            currentBatchRids.clear();

                            double percentage = (processed * 100.0) / totalRecords;
                            if (screen != null) {
                                screen.updateThread(workerIndex,
                                        String.format(java.util.Locale.forLanguageTag("es-AR"),
                                                "Class: %s | %,d / %,d (%.2f%%)", className,
                                                processed, totalRecords, percentage));
                            }
                        }
                    }
                }

                if (!"SINGLE".equalsIgnoreCase(workingMode)) {
                    if (currentBatchCount > 0 && !errorOccurred) {
                        boolean success = false;
                        for (int attempt = 1; attempt <= retryCount; attempt++) {
                            try {
                                if (isEdge) {
                                    arcadeClient.executeCommand("sqlscript", jsonlBuffer.toString());
                                } else {
                                    arcadeClient.sendBatch(jsonlBuffer.toString(), false);
                                }
                                success = true;
                                break;
                            } catch (Exception e) {
                                if (attempt == retryCount) {
                                    try (FileWriter fw = new FileWriter("migration.log", true);
                                            PrintWriter pw = new PrintWriter(fw)) {
                                        pw.printf("[%s] Error sending batch for %s after %d attempts: %s%n", threadName,
                                                className, retryCount, e.getMessage());
                                    } catch (IOException ignored) {
                                    }
                                } else {
                                    try {
                                        Thread.sleep(500 * attempt);
                                    } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                            }
                        }

                        if (success) {
                            if (!isEdge) {
                                for (String rid : currentBatchRids) {
                                    MapDBStore.addVertex(rid);
                                }
                            }
                            processed += currentBatchCount;
                            double percentage = (processed * 100.0) / totalRecords;
                            if (screen != null) {
                                screen.updateThread(workerIndex,
                                        String.format(java.util.Locale.forLanguageTag("es-AR"),
                                                "Class: %s | %,d / %,d (%.2f%%)", className,
                                                processed, totalRecords, percentage));
                            }
                        }
                    }
                }
                db.close();
            }
        }
        if (screen != null) {
            screen.updateThread(workerIndex, String.format("Finished: %s", className));
        }

        long migratedRecords = -1;
        try {
            JsonObject response = arcadeClient.executeQuery("SELECT count(*) as cnt FROM `" + className + "`");
            if (response.has("result") && response.getAsJsonArray("result").size() > 0) {
                migratedRecords = response.getAsJsonArray("result").get(0).getAsJsonObject().get("cnt").getAsLong();
            }
        } catch (Exception e) {
            System.err.println("Failed to query count for " + className + ": " + e.getMessage());
        }

        synchronized (MigrationWorker.class) {
            try (FileWriter fw = new FileWriter("resumen.log", true);
                    PrintWriter pw = new PrintWriter(fw)) {
                if (totalRecords != migratedRecords) {
                    pw.printf("%s, %d, %d <<<<<<<<%n", className, totalRecords, migratedRecords);
                } else {
                    pw.printf("%s, %d, %d%n", className, totalRecords, migratedRecords);
                }
            } catch (IOException e) {
                System.err.println("Failed to write to resumen.log: " + e.getMessage());
            }
        }

        Migrator.logProgress(threadName, className, totalRecords, screen);
    }

    private boolean processVertexToJSONL(OVertex vertex, List<String> schemaProperties, StringBuilder sb) {
        String odbRid = vertex.getIdentity().toString();

        Map<String, Object> props = new HashMap<>();
        boolean hasNonNullProperty = false;

        for (String pName : schemaProperties) {
            Object value = vertex.getProperty(pName);
            if (value != null) {
                if (value instanceof Float && ((Float) value).isNaN()) {
                    continue;
                }
                if (value instanceof Double && ((Double) value).isNaN()) {
                    continue;
                }
                props.put(pName, value);
                hasNonNullProperty = true;
            }
        }

        if (!hasNonNullProperty && !schemaProperties.isEmpty()) {
            logNullVertex(className, odbRid);
            return false;
        }

        props.put("@type", "vertex");
        props.put("@class", className);
        props.put("@id", odbRid);
        props.put("odbRID", odbRid); // Explicit property for future reference

        sb.append(gson.toJson(props)).append("\n");
        return true;
    }

    private String buildVertexSQL(OVertex vertex, List<String> schemaProperties) {
        String odbRid = vertex.getIdentity().toString();
        Map<String, Object> props = new HashMap<>();
        boolean hasNonNullProperty = false;

        props.put("odbRID", odbRid);
        for (String pName : schemaProperties) {
            Object value = vertex.getProperty(pName);
            if (value != null) {
                if (value instanceof Float && ((Float) value).isNaN()) {
                    continue;
                }
                if (value instanceof Double && ((Double) value).isNaN()) {
                    continue;
                }
                props.put(pName, value);
                hasNonNullProperty = true;
            }
        }

        if (!hasNonNullProperty && !schemaProperties.isEmpty()) {
            logNullVertex(className, odbRid);
            return null;
        }

        String content = props.isEmpty() ? "" : " CONTENT " + gson.toJson(props);
        return "CREATE VERTEX `" + className + "`" + content + ";";
    }

    private void runSingleMode(String threadName, ODatabaseSession db, List<String> schemaProperties) {
        String adbHost = config.has("ADB_HOST") ? config.get("ADB_HOST").getAsString() : "localhost";
        int adbPort = config.has("ADB_PORT") ? config.get("ADB_PORT").getAsInt() : 2480;
        int adbGRPCPort = config.has("ADB_GRPCPORT") ? config.get("ADB_GRPCPORT").getAsInt() : 50051;
        String adbDb = config.has("ADB_DB") ? config.get("ADB_DB").getAsString() : "mydb";
        String adbUser = config.has("ADB_USER") ? config.get("ADB_USER").getAsString() : "root";
        String adbPass = config.has("ADB_PASS") ? config.get("ADB_PASS").getAsString() : "root";

        com.arcadedb.remote.grpc.RemoteGrpcServer grpcServer = new com.arcadedb.remote.grpc.RemoteGrpcServer(adbHost,
                adbGRPCPort, adbUser, adbPass, false, java.util.Collections.emptyList());
        grpcServer.start();
        com.arcadedb.remote.grpc.RemoteGrpcDatabase remoteDb = new com.arcadedb.remote.grpc.RemoteGrpcDatabase(
                grpcServer, adbHost, adbGRPCPort, adbPort, adbDb, adbUser, adbPass);

        long processed = 0;
        try {
            for (com.orientechnologies.orient.core.record.OElement row : db.browseClass(className, false)) {
                String cmd = null;
                if (isEdge && row.isEdge()) {
                    StringBuilder sb = new StringBuilder();
                    if (processEdge(row.asEdge().get(), schemaProperties, sb)) {
                        cmd = sb.toString();
                    } else {
                        continue;
                    }
                } else if (!isEdge && row.isVertex()) {
                    cmd = buildVertexSQL(row.asVertex().get(), schemaProperties);
                }

                if (cmd != null) {
                    boolean success = false;
                    for (int attempt = 1; attempt <= retryCount; attempt++) {
                        try {
                            remoteDb.command("sql", cmd);
                            success = true;
                            break;
                        } catch (Exception e) {
                            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
                            if (msg.contains("ConcurrentModificationException") || msg.contains("concurrent")) {
                                if (attempt == retryCount) {
                                    logError(className, row.getIdentity().toString(),
                                            "ConcurrentModificationException after " + retryCount + " attempts: "
                                                    + msg);
                                } else {
                                    try {
                                        Thread.sleep(500 * attempt);
                                    } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                            } else {
                                logError(className, row.getIdentity().toString(), "Other exception: " + msg);
                                break;
                            }
                        }
                    }
                    if (success) {
                        processed++;
                        if (!isEdge) {
                            MapDBStore.addVertex(row.getIdentity().toString());
                        }
                        if (processed % 1000 == 0 && screen != null) {
                            double percentage = (processed * 100.0) / totalRecords;
                            screen.updateThread(workerIndex,
                                    String.format(java.util.Locale.forLanguageTag("es-AR"),
                                            "Class: %s | %,d / %,d (%.2f%%)", className,
                                            processed, totalRecords, percentage));
                        }
                    }
                }
            }
        } finally {
            try {
                remoteDb.close();
            } catch (Exception ignored) {
            }
            try {
                grpcServer.close();
            } catch (Exception ignored) {
            }

            if (screen != null && processed > 0) {
                double percentage = (processed * 100.0) / totalRecords;
                screen.updateThread(workerIndex,
                        String.format(java.util.Locale.forLanguageTag("es-AR"),
                                "Class: %s | %,d / %,d (%.2f%%)", className,
                                processed, totalRecords, percentage));
            }
        }
    }

    private boolean processEdge(OEdge edge, List<String> schemaProperties, StringBuilder sb) {
        String fromOdb = edge.getFrom().getIdentity().toString();
        String toOdb = edge.getTo().getIdentity().toString();

        boolean fromFound = MapDBStore.containsVertex(fromOdb);
        boolean toFound = MapDBStore.containsVertex(toOdb);

        if (!fromFound || !toFound) {
            try (FileWriter fw = new FileWriter("errors.log", true); PrintWriter pw = new PrintWriter(fw)) {
                pw.printf("%s - [EDGE] - from_found: %b, to_found: %b%n", edge.getIdentity().toString(), fromFound,
                        toFound);
            } catch (IOException e) {
            }
            return false;
        }

        String fromClass = edge.getFrom().getSchemaType().isPresent() ? edge.getFrom().getSchemaType().get().getName()
                : "V";
        String toClass = edge.getTo().getSchemaType().isPresent() ? edge.getTo().getSchemaType().get().getName() : "V";

        Map<String, Object> props = new HashMap<>();
        for (String pName : schemaProperties) {
            if (!pName.equals("in") && !pName.equals("out")) {
                Object value = edge.getProperty(pName);
                if (value != null) {
                    if (value instanceof Float && ((Float) value).isNaN()) {
                        continue;
                    }
                    if (value instanceof Double && ((Double) value).isNaN()) {
                        continue;
                    }
                    props.put(pName, value);
                }
            }
        }

        String contentStr = props.isEmpty() ? "" : " CONTENT " + gson.toJson(props);

        sb.append("CREATE EDGE `").append(className)
                .append("` FROM (SELECT FROM `").append(fromClass).append("` WHERE odbRID = '").append(fromOdb)
                .append("') TO (SELECT FROM `").append(toClass).append("` WHERE odbRID = '").append(toOdb)
                .append("') RETRY 5 WAIT 100").append(contentStr).append(";\n");
        return true;
    }

    private synchronized void logNullVertex(String className, String rid) {
        try (FileWriter fw = new FileWriter("NullVertex.log", true);
                PrintWriter pw = new PrintWriter(fw)) {
            pw.printf("%s - %s%n", rid, className);
        } catch (IOException e) {
            System.err.println("Failed to write to NullVertex.log: " + e.getMessage());
        }
    }

    private synchronized void logError(String className, String rid, String errorMessage) {
        try (FileWriter fw = new FileWriter("errors.log", true);
                PrintWriter pw = new PrintWriter(fw)) {
            pw.printf("%s - %s - %s%n", rid, className, errorMessage);
        } catch (IOException e) {
            System.err.println("Failed to write to errors.log: " + e.getMessage());
        }
    }
}
