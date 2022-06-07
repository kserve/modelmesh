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

import java.util.Comparator;

public final class Utils {

    private Utils() {}

    public static final Comparator<String[]> STRING_ARRAY_COMP = (l1, l2) -> {
        int diff = l1.length - l2.length;
        if (diff != 0) {
            return diff;
        }
        for (int i = 0; i < l1.length; i++) {
            if ((diff = l1[i].compareTo(l2[i])) != 0) {
                return diff;
            }
        }
        return 0;
    };

    public static <T> boolean empty(T[] arr) {
        return arr.length == 0;
    }

}
