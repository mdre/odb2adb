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
    private final MapDbStore mapDbStore;
    private final MigrationScreen screen;
    private final int workerIndex;
    private final Gson gson = new com.google.gson.GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    private final int batchSize;

    public MigrationWorker(String className, boolean isEdge, long totalRecords,
            ODatabasePool orientPool, ArcadeBatchClient arcadeClient,
            MapDbStore mapDbStore, MigrationScreen screen, int workerIndex, int batchSize) {
        this.className = className;
        this.isEdge = isEdge;
        this.totalRecords = totalRecords;
        this.orientPool = orientPool;
        this.arcadeClient = arcadeClient;
        this.mapDbStore = mapDbStore;
        this.screen = screen;
        this.workerIndex = workerIndex;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        long processed = 0;

        if (screen != null) {
            screen.updateThread(workerIndex,
                    String.format("Starting %s : %s (0 / %d)", isEdge ? "Edge" : "Vertex", className, totalRecords));
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
            StringBuilder jsonlBuffer = new StringBuilder();
            int currentBatchCount = 0;
            boolean errorOccurred = false;

            for (com.orientechnologies.orient.core.record.OElement row : db.browseClass(className, false)) {
                if (errorOccurred)
                    break;

                try {
                    if (isEdge) {
                        if (row.isEdge()) {
                            processEdgeToJSONL(row.asEdge().get(), schemaProperties, jsonlBuffer);
                            currentBatchCount++;
                        }
                    } else {
                        if (row.isVertex()) {
                            processVertexToJSONL(row.asVertex().get(), schemaProperties, jsonlBuffer);
                            currentBatchCount++;
                        }
                    }
                } catch (Exception e) {
                    String rid = row.getIdentity() != null ? row.getIdentity().toString() : "UNKNOWN";
                    logError(className, rid, e.getMessage());
                }

                if (currentBatchCount >= batchSize) {
                    try {
                        JsonObject response = arcadeClient.sendBatch(jsonlBuffer.toString(), false);

                        // For vertices, save the Mapping to MapDB
                        if (!isEdge && response.has("idMapping")) {
                            JsonObject mapping = response.getAsJsonObject("idMapping");
                            for (String tempId : mapping.keySet()) {
                                String arcadeRid = mapping.get(tempId).getAsString();
                                mapDbStore.put(tempId, arcadeRid);
                            }
                        }

                        processed += currentBatchCount;
                        currentBatchCount = 0;
                        jsonlBuffer.setLength(0);

                        double percentage = (processed * 100.0) / totalRecords;
                        if (screen != null) {
                            screen.updateThread(workerIndex, String.format("Class: %s | %d / %d (%.2f%%)", className,
                                    processed, totalRecords, percentage));
                        }
                    } catch (Exception e) {
                        try (FileWriter fw = new FileWriter("migration.log", true);
                                PrintWriter pw = new PrintWriter(fw)) {
                            pw.printf("[%s] Error sending batch for %s: %s%n", threadName, className, e.getMessage());
                        } catch (IOException ignored) {
                        }
                        errorOccurred = true;
                    }
                }
            }

            if (currentBatchCount > 0 && !errorOccurred) {
                try {
                    JsonObject response = arcadeClient.sendBatch(jsonlBuffer.toString(), false);

                    if (!isEdge && response.has("idMapping")) {
                        JsonObject mapping = response.getAsJsonObject("idMapping");
                        for (String tempId : mapping.keySet()) {
                            String arcadeRid = mapping.get(tempId).getAsString();
                            mapDbStore.put(tempId, arcadeRid);
                        }
                    }

                    processed += currentBatchCount;
                    double percentage = (processed * 100.0) / totalRecords;
                    if (screen != null) {
                        screen.updateThread(workerIndex, String.format("Class: %s | %d / %d (%.2f%%)", className,
                                processed, totalRecords, percentage));
                    }
                } catch (Exception e) {
                    try (FileWriter fw = new FileWriter("migration.log", true);
                            PrintWriter pw = new PrintWriter(fw)) {
                        pw.printf("[%s] Error sending batch for %s: %s%n", threadName, className, e.getMessage());
                    } catch (IOException ignored) {
                    }
                }
            }
            db.close();
        }
        if (screen != null) {
            screen.updateThread(workerIndex, String.format("Finished: %s", className));
        }
        Migrator.logProgress(threadName, className, totalRecords, screen);
    }

    private void processVertexToJSONL(OVertex vertex, List<String> schemaProperties, StringBuilder sb) {
        String odbRid = vertex.getIdentity().toString();

        Map<String, Object> props = new HashMap<>();
        props.put("@type", "vertex");
        props.put("@class", className);
        props.put("@id", odbRid);
        props.put("odbRID", odbRid); // Explicit property for future reference

        for (String pName : schemaProperties) {
            Object value = vertex.getProperty(pName);
            if (value != null) {
                props.put(pName, value);
            }
        }

        sb.append(gson.toJson(props)).append("\n");
    }

    private void processEdgeToJSONL(OEdge edge, List<String> schemaProperties, StringBuilder sb) {
        String fromOdb = edge.getFrom().getIdentity().toString();
        String toOdb = edge.getTo().getIdentity().toString();

        String fromArcade = mapDbStore.get(fromOdb);
        String toArcade = mapDbStore.get(toOdb);

        // Skip edges where vertices weren't migrated
        if (fromArcade == null || toArcade == null) {
            return;
        }

        Map<String, Object> props = new HashMap<>();
        props.put("@type", "edge");
        props.put("@class", className);
        props.put("@from", fromArcade);
        props.put("@to", toArcade);

        for (String pName : schemaProperties) {
            if (!pName.equals("in") && !pName.equals("out")) {
                Object value = edge.getProperty(pName);
                if (value != null) {
                    props.put(pName, value);
                }
            }
        }

        sb.append(gson.toJson(props)).append("\n");
    }

    private synchronized void logError(String className, String rid, String errorMessage) {
        try (FileWriter fw = new FileWriter("errors.log", true);
                PrintWriter pw = new PrintWriter(fw)) {
            pw.printf("[%s] Class: %s, RID: %s, Error: %s%n",
                    Thread.currentThread().getName(), className, rid, errorMessage);
        } catch (IOException e) {
            System.err.println("Failed to write to errors.log: " + e.getMessage());
        }
    }
}
