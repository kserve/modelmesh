/*
 * Copyright 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ibm.watson.modelmesh;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Parses and watches for a Kubernetes file-mounted ConfigMap key.
 * <p>
 * When mounted via a directory (without subPath option), the file itself is a
 * static symlink into a sibling dir called ..data which is itself a symlink
 * that's changed dynamically when the ConfigMap changes. So we also watch
 * for changes to this ..data dir symlink and re-resolve/load the target file
 * as appropriate.
 */
public final class ConfigMapKeyFileWatcher {
    private static final Logger logger = LogManager.getLogger(ConfigMapKeyFileWatcher.class);

    static final String DATA_SYMLINK = "..data";

    @FunctionalInterface
    public interface Parser<T> {
        T parse(Path filePath) throws IOException;
    }

    /**
     * Returns only after consumer.accept() has been called with the _initial_ config.
     *
     * @param <T>      parsed type
     * @param file     full path to mounted configmap key (i.e. configmap_mount/key_name)
     * @param consumer will be called with initial value of file, and then subsequently
     *                 whenever the ConfigMap changes
     * @param parser
     * @throws IOException
     */
    public static <T> void startWatcher(File file, Consumer<T> consumer, Parser<T> parser)
            throws IOException, InterruptedException {
        final WatchService watchService = FileSystems.getDefault().newWatchService();
        File parent = file.getParentFile();
        if (parent == null) {
            parent = file.getAbsoluteFile().getParentFile();
        }
        if (parent == null) {
            throw new IOException("Can't find parent dir for " + file);
        }
        /*final WatchKey wKey =*/
        parent.toPath().register(watchService,
                StandardWatchEventKinds.ENTRY_CREATE);
        if (!file.exists()) {
            try {
                watchService.close();
            } catch (IOException e) {
            }
            throw new IOException("Mounted configmap file not found: " + file);
        }
        final Path dataDirPath = new File(parent, DATA_SYMLINK).toPath();
        final Path filePath = file.toPath();
        final Path fileName = filePath.getFileName();
        final CountDownLatch[] latch = { new CountDownLatch(1) };
        final Thread thread = new Thread("config-watcher") {
            {
                setDaemon(true);
            }

            @Override
            public void run() {
                try {
                    final Path dataDir = dataDirPath.getFileName();
                    // Ensure we make at least one attempt to read the file
                    // before blocking for the first time (to process the initial state)
                    boolean attempted = false;
                    while (true) {
                        Exception err = null;
                        T newConfig = null;
                        WatchKey wk = attempted ? watchService.take() : null;
                        while (true) {
                            if (wk != null) {
                                for (WatchEvent<?> ev : wk.pollEvents()) {
                                    Object cxt = ev.context();
                                    if (dataDir.equals(cxt) || filePath.equals(cxt)) {
                                        try {
                                            attempted = true;
                                            T config = extractCurrentConfig();
                                            if (config != null) {
                                                newConfig = config;
                                                err = null;
                                            }
                                        } catch (Exception e) {
                                            err = e;
                                        }
                                    }
                                    if (!wk.reset()) {
                                        return; // watch service shut down
                                    }
                                }
                            }
                            if ((wk = watchService.poll()) != null) {
                                continue;
                            }
                            // This is where we ensure the config is extracted the first
                            // time through regardless of any WatchService events
                            if (newConfig == null && !attempted) {
                                try {
                                    attempted = true;
                                    newConfig = extractCurrentConfig();
                                } catch (Exception e) {
                                    err = e;
                                }
                            }
                            if (err != null) {
                                // Check again after a pause in case the err was due to
                                // symlink changing concurrently
                                Thread.sleep(500L);
                                if ((wk = watchService.poll()) != null) {
                                    continue;
                                }
                            }
                            if (newConfig != null) {
                                try {
                                    consumer.accept(newConfig);
                                } catch (Exception e) {
                                    logger.error("Error processing updated config", e);
                                } finally {
                                    triggerLatch();
                                }
                            }
                            if (err != null) {
                                logger.warn("Error reading updated configmap key from file " + file, err);
                            }
                            break; // continue outer loop
                        }
                    }
                } catch (InterruptedException e) {
                    logger.info("ConfigMap file watcher thread interrupted, exiting");
                } finally {
                    try {
                        watchService.close();
                    } catch (IOException e) {
                    }
                }
            }

            private void triggerLatch() {
                final CountDownLatch l = latch[0];
                if (l != null) {
                    l.countDown();
                    latch[0] = null;
                }
            }

            private T extractCurrentConfig() throws IOException {
                Path realFile;
                if (Files.exists(dataDirPath)) {
                    Path realDir = dataDirPath.toRealPath();
                    realFile = realDir.resolve(fileName);
                } else {
                    realFile = file.toPath();
                }
                return Files.exists(realFile) ? parser.parse(realFile) : null;
            }
        };
        thread.start();
        // Wait for config to fully update for the first time in the new thread
        // before returning from this method
        final CountDownLatch l = latch[0];
        if (l != null) {
            boolean ok = false;
            try {
                l.await(5, TimeUnit.SECONDS);
                ok = true;
            } finally {
                if (!ok) {
                    thread.interrupt();
                }
            }
        }
    }
}
