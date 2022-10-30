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

package com.ibm.watson.modelmesh.example;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ibm.watson.modelmesh.example.ExampleModelRuntime.FAST_MODE;

public class Model {

    // Keeps track of how many load attempts there have been per model id
    final static Map<String, Integer> loadCounts = new ConcurrentHashMap<>();

    final String id;

    final String location;

    long size;

    long unloadingTime;

    public Model(String id, String location) {
        this.id = id;
        this.location = location;
    }

    static final Pattern LOC_PATT = Pattern.compile("SIZE_(\\d+)_(\\d+)_(\\d+)");

    public String load() {
        int loadCount = loadCounts.merge(id, 1, (i1, i2) -> i1 + i2); // increment
        System.out.println("Loading model " + id + "(count = " + loadCount + ")...");

        Matcher m = location != null ? LOC_PATT.matcher(location) : null;

        long loadingTime;
        if (m != null && m.matches()) {
            // size and loading time to simulate encoding in location: "SIZE_<size>_<loadtime>_<unloadtime>"
            size = Long.parseLong(m.group(1));
            loadingTime = Long.parseLong(m.group(2));
            unloadingTime = Long.parseLong(m.group(3));
        } else {
            // pretend between 64MB and 128MB
            size = 1024 * 1024 * (64 + (long) (Math.random() * 64.0));
            // pretend it takes between 1 and 11 secs
            loadingTime = FAST_MODE ? 100L + (long) (Math.random() * 1000.0)
                : 1000L + (long) (Math.random() * 10000.0);
            // pretend it takes between 0 and 2 secs
            unloadingTime = (long) (Math.random() * (FAST_MODE ? 500.0 : 2000.0));
        }

        sleep(loadingTime); // simulate time to load

        if (loadCount <= 1 && location != null && location.startsWith("FAIL_")) {
            return location.substring(5); // simulate loading failure for first load
        }

        return null;
    }

    public long calculateSizeBytes() {
        System.out.println("Calculating size of model " + id + "...");
        // simulate model sizing time
        sleep(FAST_MODE ? 50L : 1000L);
        return size;
    }

    public void unload() {
        System.out.println("Unloading model " + id + "...");
        sleep(unloadingTime); // simulate time to unload
    }


    public String classify(String inputText) {
        if (!FAST_MODE) {
            sleep((long) (Math.random() * 200.0));
        }
        return "classification for " + inputText + " by model " + id;
    }


    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
