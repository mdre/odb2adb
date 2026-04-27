package com.migration.ui;

import com.googlecode.lanterna.TerminalPosition;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.graphics.TextGraphics;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.googlecode.lanterna.terminal.Terminal;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class MigrationScreen {
    private Screen screen;
    private TextGraphics textGraphics;
    
    // Global stats
    private volatile String phase = "Initializing...";
    private volatile long totalRecords = 0;
    private volatile long migratedRecords = 0;
    private volatile String estimatedTimeStr = "Calculating...";
    private volatile String globalStatus = "";
    private volatile String startTimeStr = "";
    private volatile String elapsedTimeStr = "";
    
    // Thread stats
    private final Map<Integer, String> threadStatusMap = new ConcurrentHashMap<>();
    private final int threadCount;
    private final int startRowForThreads = 8;
    
    public MigrationScreen(int threadCount) {
        this.threadCount = threadCount;
        for (int i = 0; i < threadCount; i++) {
            threadStatusMap.put(i, "Idle");
        }
    }
    
    public void init() throws IOException {
        Terminal terminal = new DefaultTerminalFactory().createTerminal();
        screen = new TerminalScreen(terminal);
        screen.startScreen();
        screen.clear();
        textGraphics = screen.newTextGraphics();
        redraw();
    }
    
    public synchronized void updateGlobal(String phase, long migrated, long total, String eta, String startTime, String elapsedTime) {
        if (phase != null) this.phase = phase;
        this.migratedRecords = migrated;
        this.totalRecords = total;
        if (eta != null) this.estimatedTimeStr = eta;
        if (startTime != null) this.startTimeStr = startTime;
        if (elapsedTime != null) this.elapsedTimeStr = elapsedTime;
        redraw();
    }
    
    public synchronized void updateStatus(String status) {
        this.globalStatus = status;
        redraw();
    }
    
    public synchronized void updateThread(int threadIndex, String status) {
        if (threadIndex >= 0 && threadIndex < threadCount) {
            threadStatusMap.put(threadIndex, status);
            redraw();
        }
    }
    
    private void redraw() {
        if (screen == null) return;
        
        try {
            screen.clear();
            
            textGraphics.setForegroundColor(TextColor.ANSI.BLUE);
            textGraphics.putString(0, 0, "=== ODB to ArcadeDB Migration Tool ===");
            
            textGraphics.setForegroundColor(TextColor.ANSI.WHITE);
            textGraphics.putString(0, 1, "Status: " + globalStatus);
            textGraphics.putString(0, 2, "Phase:  " + phase);
            textGraphics.putString(0, 3, String.format("Overall Progress: %d / %d", migratedRecords, totalRecords));
            textGraphics.putString(0, 4, "Est. End Time:    " + estimatedTimeStr);
            textGraphics.putString(0, 5, "Start Time:       " + startTimeStr);
            textGraphics.putString(0, 6, "Elapsed Time:     " + elapsedTimeStr);
            
            textGraphics.setForegroundColor(TextColor.ANSI.CYAN);
            textGraphics.putString(0, startRowForThreads, "--- Thread Status ---");
            
            textGraphics.setForegroundColor(TextColor.ANSI.WHITE);
            for (int i = 0; i < threadCount; i++) {
                String threadStr = String.format("[Worker-%d] %s", i, threadStatusMap.getOrDefault(i, "Idle"));
                textGraphics.putString(0, startRowForThreads + 1 + i, threadStr);
            }
            
            screen.refresh();
        } catch (IOException e) {
            System.err.println("Screen redraw error: " + e.getMessage());
        }
    }
    
    public void close() {
        if (screen != null) {
            try {
                screen.stopScreen();
            } catch (IOException e) {
                System.err.println("Error closing screen: " + e.getMessage());
            }
        }
    }
}
