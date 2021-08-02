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

package com.ibm.watson.modelmesh.util;

import com.ibm.watson.kvutils.KVTable.TableView;
import com.ibm.watson.modelmesh.InstanceRecord;
import com.ibm.watson.modelmesh.ModelRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map.Entry;

/**
 * Class which models instance state
 *
 * @author nrao
 */
public class InstanceStateUtil {

    private static final Logger logger = LogManager.getLogger(InstanceStateUtil.class);

    public static void logModelRegistry(TableView<ModelRecord> registry) {

        logger.info("*** ModelRegistryTable ***");
        boolean entriesExistFlag = false;
        int i = 0;
        for (Entry<String, ModelRecord> e : registry) {
            if (entriesExistFlag == false) {
                entriesExistFlag = true;
            }
            logger.info("<" + ++i + "[" + e.getKey() + "]:" + e.getValue().toString() + ">");
        }
        if (entriesExistFlag == false) {
            logger.info("Table empty!");
        }
    }

    public static void logInstanceInfo(TableView<InstanceRecord> instanceInfo) {

        logger.info("*** InstanceInfoTable ***");
        boolean entriesExistFlag = false;
        int i = 0;
        for (Entry<String, InstanceRecord> e : instanceInfo) {
            if (entriesExistFlag == false) {
                entriesExistFlag = true;
            }
            logger.info("<" + ++i + "> <InstanceId [" + e.getKey() + "]:" + e.getValue().toString() + ">");
        }
        if (entriesExistFlag == false) {
            logger.info("Table empty!");
        }
    }
}
