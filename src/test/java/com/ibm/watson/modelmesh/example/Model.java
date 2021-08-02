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

import static com.ibm.watson.modelmesh.example.ExampleModelRuntime.FAST_MODE;

public class Model {

    final String id;

    final String location;

    long size;

    public Model(String id, String location) {
        this.id = id;
        this.location = location;
    }

    public String load() {
        System.out.println("Loading model " + id + "...");
        // pretend it takes between 1 and 11 secs
        //TODO make this also vary based on the size
        if (FAST_MODE) {
            sleep(100L + (long) (Math.random() * 1000.0));
        } else {
            sleep(1000L + (long) (Math.random() * 10000.0));
        }

        if (location != null && location.startsWith("FAIL_")) {
            return location.substring(5); // simulate loading failure
        }

        // pretend between 64MB and 128MB
        size = 1024 * 1024 * (64 + (long) (Math.random() * 64.0));
        return null;
    }

    public long calculateSizeBytes() {
        System.out.println("Calculating size of model " + id + "...");
        // pretend it takes 1.5 secs
        sleep(FAST_MODE ? 50L : 1500L);
        return size;
    }

    public void unload() {
        System.out.println("Unloading model " + id + "...");
        // pretend it takes between 0 and 2 secs
        sleep((long) (Math.random() * (FAST_MODE ? 500.0 : 2000.0)));
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
