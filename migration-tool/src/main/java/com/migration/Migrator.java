package com.migration;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.index.OIndex;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.io.FileReader;
import java.io.IOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.concurrent.atomic.AtomicLong;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import com.migration.ui.MigrationScreen;

public class Migrator {

    public static LocalDateTime startTime;
    public static long startTimeMillis;
    public static AtomicLong recordsMigrated = new AtomicLong(0);
    public static AtomicLong totalRecordsToMigrate = new AtomicLong(0);
    public static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void logProgress(String threadName, String className, long processedRecords, MigrationScreen screen) {
        long completed = recordsMigrated.addAndGet(processedRecords);
        long elapsed = System.currentTimeMillis() - startTimeMillis;
        double avgMsPerRecord = elapsed > 0 && completed > 0 ? (double) elapsed / completed : 0;

        long totalRecords = totalRecordsToMigrate.get();
        double estimatedTotalHours = (avgMsPerRecord * totalRecords) / (1000.0 * 60 * 60);
        long remainingMs = (long) (avgMsPerRecord * (Math.max(0, totalRecords - completed)));
        LocalDateTime estimatedEndTime = LocalDateTime.now().plus(remainingMs, ChronoUnit.MILLIS);

        String elapsedTimeStr = String.format("%02d:%02d:%02d", elapsed / 3600000, (elapsed / 60000) % 60,
                (elapsed / 1000) % 60);

        if (screen != null) {
            String eta = estimatedEndTime.format(dtf);
            screen.updateGlobal(null, completed, totalRecords, eta, startTime.format(dtf), elapsedTimeStr);
        } else {
            System.out.printf("[%s] Finished migrating %s (%d records). Completed: %d/%d records.%n",
                    threadName, className, processedRecords, completed, totalRecords);
            System.out.printf("   -> Est. total hours: %.2f | Est. End Time: %s%n", estimatedTotalHours,
                    estimatedEndTime.format(dtf));
        }
    }

    private static void printHelp() {
        System.out.println("Usage: java -jar odb2adb.jar [options] [configPath]");
        System.out.println("Options:");
        System.out.println("  -help, -h          Show this help message");
        System.out.println("  -so                Schema Only mode. Creates the schema and exits.");
        System.out.println("  -createSchema      Create schema in ArcadeDB before migration.");
        System.out.println("  -checkMigration    Check data consistency between OrientDB and ArcadeDB.");
        System.out.println("  -debug             Enable debug mode and logging.");
        System.out.println(
                "  -dumpSchema <name> Dump the ArcadeDB schema creation script to <name>_schemaonly.sql and <name>_index.sql.");
        System.out.println("  -migrate           Start the record migration process.");
        System.out.println("  -vertexOnly        Migrate only vertices.");
        System.out.println("  -edgeOnly          Migrate only edges.");
        System.out.println(
                "  -listTargetSourceClass List target and source classes with record counts to targetSourceClass.log.");
        System.out.println("  [configPath]       Path to the config.json file (default: config.json).");
    }

    public static void main(String[] args) {
        boolean schemaOnly = false;
        boolean checkMigration = false;
        boolean createSchema = false;
        boolean migrate = false;
        boolean vertexOnly = false;
        boolean edgeOnly = false;
        boolean listTargetSourceClass = false;
        String dumpSchemaPath = null;
        String configPath = "config.json";
        boolean debug = false;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equalsIgnoreCase("-help") || arg.equalsIgnoreCase("-h")) {
                printHelp();
                return;
            } else if (arg.equalsIgnoreCase("-so")) {
                schemaOnly = true;
                createSchema = true;
            } else if (arg.equalsIgnoreCase("-createSchema")) {
                createSchema = true;
            } else if (arg.equalsIgnoreCase("-migrate")) {
                migrate = true;
            } else if (arg.equalsIgnoreCase("-checkMigration")) {
                checkMigration = true;
            } else if (arg.equalsIgnoreCase("-vertexOnly")) {
                vertexOnly = true;
            } else if (arg.equalsIgnoreCase("-edgeOnly")) {
                edgeOnly = true;
            } else if (arg.equalsIgnoreCase("-listTargetSourceClass")) {
                listTargetSourceClass = true;
            } else if (arg.equalsIgnoreCase("-debug")) {
                debug = true;
            } else if (arg.equalsIgnoreCase("-dumpSchema") && i + 1 < args.length) {
                dumpSchemaPath = args[++i];
                schemaOnly = true;
                createSchema = true;
            } else if (!arg.startsWith("-")) {
                configPath = arg;
            }
        }

        JsonObject config;
        try (FileReader reader = new FileReader(configPath)) {
            config = JsonParser.parseReader(reader).getAsJsonObject();
        } catch (IOException e) {
            System.err.println("Could not load config file: " + configPath);
            return;
        }

        String odbUrl = config.has("ODB_URL") ? config.get("ODB_URL").getAsString() : "remote:localhost";
        String odbDb = config.has("ODB_DB") ? config.get("ODB_DB").getAsString() : "mydb";
        String odbUser = config.has("ODB_USER") ? config.get("ODB_USER").getAsString() : "root";
        String odbPass = config.has("ODB_PASS") ? config.get("ODB_PASS").getAsString() : "root";

        String adbHost = config.has("ADB_HOST") ? config.get("ADB_HOST").getAsString() : "localhost";
        int adbPort = config.has("ADB_PORT") ? config.get("ADB_PORT").getAsInt() : 2480;
        int adbGRPCPort = config.has("ADB_GRPCPORT") ? config.get("ADB_GRPCPORT").getAsInt() : 50051;
        String adbDb = config.has("ADB_DB") ? config.get("ADB_DB").getAsString() : "mydb";
        String adbUser = config.has("ADB_USER") ? config.get("ADB_USER").getAsString() : "root";
        String adbPass = config.has("ADB_PASS") ? config.get("ADB_PASS").getAsString() : "root";

        int threadCount = config.has("THREADS") ? config.get("THREADS").getAsInt() : 4;
        int vertexBatchSize = config.has("VERTEX_BATCH_SIZE") ? config.get("VERTEX_BATCH_SIZE").getAsInt() : 50000;
        int edgeBatchSize = config.has("EDGE_BATCH_SIZE") ? config.get("EDGE_BATCH_SIZE").getAsInt() : 50000;
        int retryCount = config.has("RETRY_COUNT") ? config.get("RETRY_COUNT").getAsInt() : 3;

        startTime = LocalDateTime.now();
        startTimeMillis = System.currentTimeMillis();
        System.out.println("Starting Migration Process at: " + startTime.format(dtf));

        OrientDB orientDB = new OrientDB(odbUrl, odbUser, odbPass, OrientDBConfig.defaultConfig());
        ODatabasePool pool = new ODatabasePool(orientDB, odbDb, odbUser, odbPass);

        ArcadeBatchClient arcadeClient = new ArcadeBatchClient(adbHost, adbPort, adbDb, adbUser, adbPass);
        arcadeClient.setDebug(debug);

        MigrationScreen screen = new MigrationScreen(threadCount);
        try {
            screen.init();
            screen.updateStatus("Connecting to databases...");
        } catch (Exception e) {
            System.err.println("Failed to start TUI: " + e.getMessage());
            screen = null; // fallback to stdout if TUI fails? No wait, we already stripped stdout on
                           // workers.
        }

        if (screen != null)
            screen.updateStatus("Extracting schema from OrientDB...");
        System.out.println("Connected. Extracting schema from OrientDB...");

        List<OClass> vertexClasses = new ArrayList<>();
        List<OClass> edgeClasses = new ArrayList<>();

        List<String> skipClasses = new ArrayList<>();
        if (config.has("SKIP")) {
            config.get("SKIP").getAsJsonArray().forEach(e -> skipClasses.add(e.getAsString()));
        }

        List<String> onlyThisClasses = new ArrayList<>();
        if (config.has("ONLY_THIS")) {
            config.get("ONLY_THIS").getAsJsonArray().forEach(e -> onlyThisClasses.add(e.getAsString()));
        }

        try (ODatabaseSession session = pool.acquire()) {
            if (listTargetSourceClass) {
                new java.io.File("targetSourceClass.log").delete();
            }
            for (OClass oClass : session.getMetadata().getSchema().getClasses()) {
                String className = oClass.getName();
                boolean toImport = true;
                String ignoreReason = null;

                if (listTargetSourceClass) {
                    long count = session.countClass(className, false);
                    try (java.io.FileWriter fw = new java.io.FileWriter("targetSourceClass.log", true);
                            java.io.PrintWriter pw = new java.io.PrintWriter(fw)) {
                        pw.printf("[%s] %s, Records: %d%n", oClass.isEdgeType() ? "EDGE" : "VERTEX", className, count);
                    } catch (IOException e) {
                        System.err.println("Failed to write to targetSourceClass.log: " + e.getMessage());
                    }
                }

                if (skipClasses.contains(className)) {
                    toImport = false;
                    ignoreReason = "In SKIP list";
                } else if (!onlyThisClasses.isEmpty() && !onlyThisClasses.contains(className)) {
                    toImport = false;
                    ignoreReason = "Not in ONLY_THIS list";
                }

                if (!toImport) {
                    String classType = oClass.isSubClassOf("V") ? "Vertex"
                            : (oClass.isSubClassOf("E") ? "Edge" : "Unknown");
                    try (java.io.FileWriter fw = new java.io.FileWriter("ignoredclass.log", true);
                            java.io.PrintWriter pw = new java.io.PrintWriter(fw)) {
                        pw.printf("[%s] Class: %s ignored - Reason: %s%n", classType, className, ignoreReason);
                    } catch (IOException e) {
                        System.err.println("Failed to write to ignoredclass.log: " + e.getMessage());
                    }
                }

                if (oClass.isSubClassOf("V") && !className.equals("V")) {
                    vertexClasses.add(oClass);
                    if (toImport)
                        totalRecordsToMigrate.addAndGet(session.countClass(className, false));
                } else if (oClass.isSubClassOf("E") && !className.equals("E")) {
                    edgeClasses.add(oClass);
                    if (toImport)
                        totalRecordsToMigrate.addAndGet(session.countClass(className, false));
                }
            }
        }

        vertexClasses = sortTopologically(vertexClasses);
        edgeClasses = sortTopologically(edgeClasses);

        if (checkMigration) {
            System.out.println("Running migration check...");
            com.arcadedb.remote.grpc.RemoteGrpcServer grpcServer = null;
            com.arcadedb.remote.grpc.RemoteGrpcDatabase remoteDb = null;
            try (ODatabaseSession session = pool.acquire();
                    java.io.PrintWriter pw = new java.io.PrintWriter(new java.io.FileWriter("checkStatus.log"))) {
                grpcServer = new com.arcadedb.remote.grpc.RemoteGrpcServer(adbHost, adbGRPCPort, adbUser, adbPass,
                        false,
                        java.util.Collections.emptyList());
                grpcServer.start();
                remoteDb = new com.arcadedb.remote.grpc.RemoteGrpcDatabase(grpcServer, adbHost, adbGRPCPort, adbPort,
                        adbDb, adbUser,
                        adbPass);
                for (OClass oClass : session.getMetadata().getSchema().getClasses()) {
                    String className = oClass.getName();
                    if (skipClasses.contains(className)
                            || (!onlyThisClasses.isEmpty() && !onlyThisClasses.contains(className))) {
                        continue;
                    }
                    if ((oClass.isSubClassOf("V") && !className.equals("V"))
                            || (oClass.isSubClassOf("E") && !className.equals("E"))) {
                        long odbCount = session.countClass(className, false);
                        long adbCount = -1;
                        try {
                            com.arcadedb.query.sql.executor.ResultSet result = remoteDb.query("sql",
                                    "SELECT count(*) as cnt FROM `" + className + "`");
                            if (result.hasNext()) {
                                adbCount = ((Number) result.next().getProperty("cnt")).longValue();
                            }
                            if (odbCount != adbCount) {
                                pw.printf("Class: %s, OrientDB: %d, ArcadeDB: %d%n", className, odbCount, adbCount);
                                System.out.printf(
                                        "Class: %s, OrientDB: %d, ArcadeDB: %d  <<<<<<<< ERROR: no coincide la cantidad de registros %n",
                                        className, odbCount, adbCount);
                            } else {
                                System.out.printf(
                                        "Class: %s, OrientDB: %d, ArcadeDB: %d >>> Ok! %n   ",
                                        className, odbCount, adbCount);
                            }
                        } catch (Exception e) {
                            System.err.println(
                                    "Failed to query count for " + className + " in ArcadeDB: " + e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error during checkMigration: " + e.getMessage());
            } finally {
                if (remoteDb != null) {
                    remoteDb.close();
                }
                if (grpcServer != null) {
                    grpcServer.close();
                }
            }
            System.out.println("Check migration completed. See checkStatus.log for details.");
            if (screen != null)
                screen.close();
            pool.close();
            orientDB.close();
            return;
        }

        if (screen != null)
            screen.updateGlobal("Initializing Schema", 0, totalRecordsToMigrate.get(), "...", startTime.format(dtf),
                    "00:00:00");

        if (createSchema) {
            if (screen != null)
                screen.updateStatus(
                        dumpSchemaPath != null ? "Dumping schema to file..." : "Initializing schema in ArcadeDB...");
            System.out.println(dumpSchemaPath != null ? "Dumping schema to file: " + dumpSchemaPath
                    : "Initializing schema in ArcadeDB...");
            java.io.PrintWriter dumpWriter = null;
            java.io.PrintWriter indexWriter = null;
            try {
                if (dumpSchemaPath != null) {
                    dumpWriter = new java.io.PrintWriter(new java.io.FileWriter(dumpSchemaPath + "_schemaonly.sql"));
                    indexWriter = new java.io.PrintWriter(new java.io.FileWriter(dumpSchemaPath + "_index.sql"));
                }
                try (ODatabaseSession session = pool.acquire()) {
                    for (OClass vClass : vertexClasses) {
                        List<String> superTypes = new ArrayList<>();
                        for (OClass superClass : vClass.getSuperClasses()) {
                            if (!superClass.getName().equals("V")) {
                                superTypes.add("`" + superClass.getName() + "`");
                            }
                        }
                        String extendsClause = superTypes.isEmpty() ? "" : " EXTENDS " + String.join(", ", superTypes);
                        String cmd1 = "CREATE VERTEX TYPE `" + vClass.getName() + "`" + " IF NOT EXISTS"
                                + extendsClause;
                        if (dumpWriter != null) {
                            dumpWriter.println(cmd1 + ";");
                        } else {
                            arcadeClient.executeCommand(cmd1);
                        }
                        if (superTypes.isEmpty()) {
                            String cmd2 = "CREATE PROPERTY `" + vClass.getName() + "`.odbRID IF NOT EXISTS STRING";
                            String cmd3 = "CREATE INDEX IF NOT EXISTS ON `" + vClass.getName() + "` (odbRID) NOTUNIQUE";
                            if (dumpWriter != null) {
                                dumpWriter.println(cmd2 + ";");
                                // este índice se crea facilitar el proceso de migración y no es estrictamente
                                // necesario
                                dumpWriter.println(cmd3 + ";");
                            } else {
                                arcadeClient.executeCommand(cmd2);
                                arcadeClient.executeCommand(cmd3);
                            }
                        }
                        createPropertiesAndIndexes(vClass, arcadeClient, dumpWriter, indexWriter);
                    }
                    for (OClass eClass : edgeClasses) {
                        List<String> superTypes = new ArrayList<>();
                        for (OClass superClass : eClass.getSuperClasses()) {
                            if (!superClass.getName().equals("E")) {
                                superTypes.add("`" + superClass.getName() + "`");
                            }
                        }
                        String extendsClause = superTypes.isEmpty() ? "" : " EXTENDS " + String.join(", ", superTypes);
                        String cmd1 = "CREATE EDGE TYPE `" + eClass.getName() + "`" + " IF NOT EXISTS " + extendsClause;
                        if (dumpWriter != null) {
                            dumpWriter.println(cmd1 + ";");
                        } else {
                            arcadeClient.executeCommand(cmd1);
                        }
                        createPropertiesAndIndexes(eClass, arcadeClient, dumpWriter, indexWriter);
                    }
                }
            } catch (Exception e) {
                if (screen != null)
                    screen.close();
                System.err.println("Fatal: Could not initialize ArcadeDB schema: " + e.getMessage());
                e.printStackTrace();
                return;
            } finally {
                if (dumpWriter != null) {
                    dumpWriter.close();
                }
                if (indexWriter != null) {
                    indexWriter.close();
                }
            }
        }

        if (schemaOnly) {
            if (screen != null) {
                screen.updateStatus("Schema Initialization Completed (Schema Only Mode).");
                long elapsed = System.currentTimeMillis() - startTimeMillis;
                String elapsedTimeStr = String.format("%02d:%02d:%02d", elapsed / 3600000, (elapsed / 60000) % 60,
                        (elapsed / 1000) % 60);
                screen.updateGlobal("Done", 0, 0, "Finished", startTime.format(dtf), elapsedTimeStr);
                try {
                    Thread.sleep(3000);
                } catch (Exception ignored) {
                }
                screen.close();
            }
            System.out.println("Schema Initialization Completed Successfully (Schema Only Mode).");
            pool.close();
            orientDB.close();
            return;
        }

        MapDBStore.init();
        // Phase 1: Migrating Vertices
        if (migrate && !edgeOnly) {
            if (screen != null) {
                screen.updateStatus("Migrating data...");
                long elapsed = System.currentTimeMillis() - startTimeMillis;
                String elapsedTimeStr = String.format("%02d:%02d:%02d", elapsed / 3600000, (elapsed / 60000) % 60,
                        (elapsed / 1000) % 60);
                screen.updateGlobal("Migrating Vertices", 0, totalRecordsToMigrate.get(), "Calculating...",
                        startTime.format(dtf), elapsedTimeStr);
            }
            System.out.println("---- Phase 1: Migrating Vertices ----");
            runWorkers(vertexClasses, false, threadCount, pool, arcadeClient, skipClasses, onlyThisClasses,
                    screen, vertexBatchSize, retryCount, config);
        }

        // Phase 2: Migrating Edges
        if (migrate && !vertexOnly) {
            if (screen != null) {
                long elapsed = System.currentTimeMillis() - startTimeMillis;
                String elapsedTimeStr = String.format("%02d:%02d:%02d", elapsed / 3600000, (elapsed / 60000) % 60,
                        (elapsed / 1000) % 60);
                screen.updateGlobal("Migrating Edges", recordsMigrated.get(), totalRecordsToMigrate.get(),
                        "Calculating...", startTime.format(dtf), elapsedTimeStr);
            }
            System.out.println("---- Phase 2: Migrating Edges ----");
            runWorkers(edgeClasses, true, threadCount, pool, arcadeClient, skipClasses, onlyThisClasses,
                    screen, edgeBatchSize, retryCount, config);
        }

        LocalDateTime endTime = LocalDateTime.now();

        if (screen != null) {
            screen.updateStatus("Migration Completed Successfully!");
            long elapsed = System.currentTimeMillis() - startTimeMillis;
            String elapsedTimeStr = String.format("%02d:%02d:%02d", elapsed / 3600000, (elapsed / 60000) % 60,
                    (elapsed / 1000) % 60);
            screen.updateGlobal("Done", recordsMigrated.get(), totalRecordsToMigrate.get(),
                    "Finished at " + endTime.format(dtf), startTime.format(dtf), elapsedTimeStr);
            try {
                Thread.sleep(3000);
            } catch (Exception ignored) {
            }
            screen.close();
        }

        System.out.println("Migration Completed Successfully at: " + endTime.format(dtf));

        pool.close();
        orientDB.close();
        MapDBStore.close();
    }

    private static List<OClass> sortTopologically(List<OClass> classes) {
        List<OClass> sorted = new ArrayList<>();
        List<OClass> remaining = new ArrayList<>(classes);
        while (!remaining.isEmpty()) {
            boolean progress = false;
            java.util.Iterator<OClass> it = remaining.iterator();
            while (it.hasNext()) {
                OClass c = it.next();
                boolean hasUnresolvedSuper = false;
                for (OClass other : remaining) {
                    if (c != other && c.isSubClassOf(other)) {
                        hasUnresolvedSuper = true;
                        break;
                    }
                }
                if (!hasUnresolvedSuper) {
                    sorted.add(c);
                    it.remove();
                    progress = true;
                }
            }
            if (!progress) {
                sorted.addAll(remaining);
                break;
            }
        }
        return sorted;
    }

    private static void runWorkers(List<OClass> classes, boolean isEdge, int threadCount,
            ODatabasePool pool, ArcadeBatchClient arcadeClient,
            List<String> skipClasses, List<String> onlyThisClasses, MigrationScreen screen, int batchSize,
            int retryCount, JsonObject config) {
        // Find which classes need importing:
        List<OClass> importClasses = new ArrayList<>();
        for (OClass oClass : classes) {
            String className = oClass.getName();
            boolean toImport = true;
            if (skipClasses.contains(className)) {
                toImport = false;
            } else if (!onlyThisClasses.isEmpty() && !onlyThisClasses.contains(className)) {
                toImport = false;
            }
            if (toImport) {
                importClasses.add(oClass);
            }
        }

        if (importClasses.isEmpty()) {
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        try (ODatabaseSession session = pool.acquire()) {
            int taskIndex = 0;
            for (OClass oClass : importClasses) {
                long count = session.countClass(oClass.getName(), false);
                if (count > 0) {
                    int workerIndex = taskIndex % threadCount;
                    executor.submit(new MigrationWorker(oClass.getName(), isEdge, count, pool, arcadeClient,
                            screen, workerIndex, batchSize, retryCount, config));
                    taskIndex++;
                } else {
                    String classType = isEdge ? "Edge" : "Vertex";
                    try (java.io.FileWriter fw = new java.io.FileWriter("ignoredclass.log", true);
                            java.io.PrintWriter pw = new java.io.PrintWriter(fw)) {
                        pw.printf("[%s] Class: %s ignored - Reason: 0 records%n", classType, oClass.getName());
                    } catch (IOException e) {
                        System.err.println("Failed to write to ignoredclass.log: " + e.getMessage());
                    }
                }
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(7, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void createPropertiesAndIndexes(OClass oClass, ArcadeBatchClient arcadeClient,
            java.io.PrintWriter dumpWriter, java.io.PrintWriter indexWriter) {
        for (OProperty prop : oClass.declaredProperties()) {
            try {
                String propType = prop.getType().name();

                // Traducir de Orient a Arcade
                if (propType.equals("EMBEDDEDLIST")) {
                    propType = "LIST";
                } else if (propType.equals("EMBEDDEDSET")) {
                    propType = "LIST";
                } else if (propType.equals("EMBEDDEDMAP")) {
                    propType = "MAP";
                }

                String cmd = "CREATE PROPERTY `" + oClass.getName() + "`.`" + prop.getName() + "` IF NOT EXISTS "
                        + propType;
                if (dumpWriter != null) {
                    dumpWriter.println(cmd + ";");
                } else {
                    arcadeClient.executeCommand(cmd);
                }

            } catch (Exception e) {
                System.err.println("Warning: Could not create property " + prop.getName() + " for class "
                        + oClass.getName() + ": " + e.getMessage());
                e.printStackTrace();
            }

        }
        for (OIndex idx : oClass.getIndexes()) {
            try {
                String idxType = idx.getType();
                if (idxType.contains("UNIQUE") || idxType.contains("NOTUNIQUE") || idxType.contains("FULLTEXT")) {
                    String baseType = idxType.contains("NOTUNIQUE") ? "NOTUNIQUE"
                            : (idxType.contains("UNIQUE") ? "UNIQUE" : "FULL_TEXT");
                    java.util.List<String> fields = idx.getDefinition().getFields();
                    if (!fields.isEmpty()) {
                        java.util.List<String> processedFields = new java.util.ArrayList<>();
                        for (String field : fields) {
                            OProperty prop = oClass.getProperty(field);
                            if (prop != null && (prop.getType().name().equals("EMBEDDEDLIST")
                                    || prop.getType().name().equals("EMBEDDEDSET"))) {
                                processedFields.add(field + " BY ITEM");
                            } else {
                                processedFields.add(field);
                            }
                        }
                        String fieldsStr = String.join(", ", processedFields);
                        String idxName = idx.getName().replace(".", "_");
                        String cmd = "CREATE INDEX `" + idxName + "` IF NOT EXISTS ON `" + oClass.getName() + "` ("
                                + fieldsStr + ") "
                                + baseType;
                        if (indexWriter != null) {
                            indexWriter.println(cmd + ";");
                        } else {
                            arcadeClient.executeCommand(cmd);
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println(
                        "Warning: Could not create index for class " + oClass.getName() + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
