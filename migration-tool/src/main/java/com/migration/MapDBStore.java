package com.migration;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import java.util.concurrent.ConcurrentMap;

public class MapDBStore {
    private static DB db;
    private static ConcurrentMap<String, Boolean> vertexMap;

    public static void init() {
        if (db == null) {
            db = DBMaker.fileDB("vertex_rids.mapdb")
                    .fileMmapEnableIfSupported()
                    .fileMmapPreclearDisable()
                    .cleanerHackEnable()
                    .closeOnJvmShutdown()
                    .make();
            vertexMap = db.hashMap("vertexMap", Serializer.STRING, Serializer.BOOLEAN).createOrOpen();
        }
    }

    public static void addVertex(String rid) {
        if (vertexMap != null) {
            vertexMap.put(rid, true);
        }
    }

    public static boolean containsVertex(String rid) {
        if (vertexMap != null) {
            return vertexMap.containsKey(rid);
        }
        return false;
    }

    public static void close() {
        if (db != null && !db.isClosed()) {
            db.close();
        }
    }
}
