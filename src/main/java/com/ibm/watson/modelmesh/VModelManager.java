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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.ibm.etcd.client.GrpcClient;
import com.ibm.watson.kvutils.JsonSerializer;
import com.ibm.watson.kvutils.KVRecord;
import com.ibm.watson.kvutils.KVTable;
import com.ibm.watson.kvutils.KVTable.Helper.TableTxn;
import com.ibm.watson.kvutils.KVTable.TableView;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import com.ibm.watson.litelinks.ThreadPoolHelper;
import com.ibm.watson.modelmesh.GrpcSupport.InterruptingListener;
import com.ibm.watson.modelmesh.api.ModelInfo;
import com.ibm.watson.modelmesh.api.ModelStatusInfo;
import com.ibm.watson.modelmesh.api.ModelStatusInfo.ModelStatus;
import com.ibm.watson.modelmesh.api.SetVModelRequest;
import com.ibm.watson.modelmesh.api.VModelStatusInfo;
import com.ibm.watson.modelmesh.api.VModelStatusInfo.VModelStatus;
import com.ibm.watson.modelmesh.thrift.ModelNotFoundException;
import com.ibm.watson.modelmesh.thrift.StatusInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.ibm.watson.modelmesh.ModelMeshApi.BALANCED_CTX_KEY;
import static com.ibm.watson.modelmesh.thrift.Status.NOT_FOUND;
import static java.lang.System.currentTimeMillis;

/**
 * This class contains logic related to VModels (virtual or versioned models)
 */
public final class VModelManager implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(VModelManager.class);

    private static final int VMODEL_REGISTRY_BUCKETS = 128;

    static final ModelInfo EMPTY_MI = ModelInfo.getDefaultInstance();

    private static final VModelRecord UNSET = new VModelRecord();

    // This is a way to pin the owner field for future- delpoyments. Future updates will incorporate
    // owner into VModel APIs - to specify per vmodel and check owner of a particular vmodel
    private static final String DEFAULT_OWNER = System.getenv(ModelMeshEnvVars.DEFAULT_VMODEL_OWNER);

    static {
        if (DEFAULT_OWNER != null) {
            logger.info("Owner for all created vmodels will be \"" + DEFAULT_OWNER + "\"");
        } else {
            logger.info("No default vmodel owner is set");
        }
    }

    final ModelMesh modelMesh;

    // this class only "owns" the vmodel table
    final KVTable vModelTable;
    final TableView<VModelRecord> vModels;

    final TableView<ModelRecord> registry;
    final KVTable.Helper kvHelper;

    public VModelManager(ModelMesh modelMesh, String vModelsPath) throws Exception {
        this.modelMesh = modelMesh;
        this.registry = modelMesh.getRegistry();

        KVUtilsFactory kvFactory = modelMesh.getKvUtilsFactory();

        kvHelper = kvFactory.KVTableHelper();
        vModelTable = kvFactory.newKVTable(vModelsPath, VMODEL_REGISTRY_BUCKETS);

        vModels = vModelTable.getView(new JsonSerializer<>(VModelRecord.class), 1);
        vModels.addListener(false, VMODEL_LISTENER);
        vModels.addListener(false, (event, vmid, vmr) -> {
            // only run in leader
            if (event == KVTable.EventType.ENTRY_DELETED || !this.modelMesh.isLeader()) {
                return;
            }
            vModelProcExecutor.execute(() -> processVModel(vmid, vmr));
        });
    }

    public ListenableFuture<Boolean> start() {
        return vModelTable.start();
    }

    @Override
    public void close() {
        try {
            vModelTable.close();
            targetScaleupExecutor.shutdown();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Logic for add and set-target methods
     *
     * @param request
     * @throws Exception
     */
    public VModelStatusInfo updateVModel(SetVModelRequest request) throws Exception {
        final String vmid = request.getVModelId(), targetModelId = request.getTargetModelId();
        final ModelInfo modelInfo = EMPTY_MI.equals(request.getModelInfo()) ? null : request.getModelInfo();
        final boolean updateOnly = request.getUpdateOnly(), autoDelete = request.getAutoDeleteTargetModel();
        final boolean force = request.getForce(), loadNow = request.getLoadNow(), sync = request.getSync();
        final String expectedTargetModelId = emptyToNull(request.getExpectedTargetModelId());
        final String owner = emptyToNull(request.getOwner());

        /* We currently ignore the case where the same model exists with same attributes
         * _apart from_ the autoDelete flag being different. In this case the original
         * value of that flag will continue to take effect regardless of its value in this
         * request. I don't think this will be significant in practice, but we might
         * update the behaviour in future to update the flag.
         */

        if (isNullOrEmpty(vmid)) {
            throw error(Status.INVALID_ARGUMENT, "Must provide vModelId");
        }
        if (isNullOrEmpty(targetModelId)) {
            throw error(Status.INVALID_ARGUMENT, "Must provide target modelId for vmodel " + vmid);
        }

        String type, key, path;
        if (modelInfo == null) {
            type = key = path = null;
        } else {
            type = modelMesh.validateNewModelType(modelInfo.getType()); // throws IIE
            key = emptyToNull(modelInfo.getKey());
            path = emptyToNull(modelInfo.getPath());
        }

        final long initialCacheTimestamp = currentTimeMillis()
                // set record lastUsed age to 6x older if we're not loading now
                - (ModelMesh.LASTUSED_AGE_ON_ADD_MS * (loadNow ? 1L : 6L));

        VModelRecord vmr = UNSET;

        if (expectedTargetModelId != null) {
            vmr = vModels.getStrong(vmid);
            if (vmr == null) {
                if (!expectedTargetModelId.equals(targetModelId)) {
                    throw error(Status.FAILED_PRECONDITION, "vmodel " + vmid + " not found");
                }
                // if expectedTarget == target in request, the vmodel does not have to exist
            } else if (owner != null && !Objects.equals(vmr.getOwner(), owner)) {
                throw error(Status.ALREADY_EXISTS, "vmodel " + vmid
                                                   + " already exists with different owner \"" + owner + "\"");
            } else if (!expectedTargetModelId.equals(vmr.getTargetModel())
                       && !targetModelId.equals(vmr.getTargetModel())) {
                throw error(Status.FAILED_PRECONDITION, "Existing target model " + vmr.getTargetModel()
                                                        + " does not match expected " + expectedTargetModelId);
            }
        }

        ModelRecord targetModel = registry.getOrStrongIfAbsent(targetModelId);

        boolean weCreatedTargetModel, weCreatedVModel;
        ModelRecord prevActive;
        int targetCopyCount;
        List<String> autoDeleted = new ArrayList<>(2);
        while (true) {
            weCreatedTargetModel = false;
            weCreatedVModel = false;
            prevActive = null;
            targetCopyCount = 0;
            if (modelInfo == null) {
                if (targetModel == null) {
                    throw error(Status.NOT_FOUND, "Target model " + targetModelId + " not found");
                }
            } else if (targetModel != null) {
                // compare provided modelInfo with existing info
                if (!Objects.equals(type, targetModel.getType())
                    || !Objects.equals(path, targetModel.getModelPath())
                    || !Objects.equals(key, targetModel.getEncryptionKey())) {
                    // doesn't match
                    throw error(Status.ALREADY_EXISTS,
                            "Target model " + targetModelId + " already exists with different attribute values");
                }
            }
            if (vmr == UNSET) {
                vmr = vModels.getStrong(vmid);
            }
            String prevTargetIdToDelete = null, prevActiveIdToDelete = null;
            if (vmr != null) {
                // vmodel with this id already exists
                if (owner != null && !Objects.equals(vmr.getOwner(), owner)) {
                    throw error(Status.ALREADY_EXISTS, "vmodel " + vmid
                                                       + " already exists with different owner \"" + vmr.getOwner() +
                                                       "\"");
                }
                String prevTargetId = vmr.getTargetModel();
                String prevActiveId = vmr.getActiveModel();
                boolean targetMatches = targetModelId.equals(prevTargetId);
                boolean activeMatches = targetModelId.equals(prevActiveId);
                if (!targetMatches) {
                    if (expectedTargetModelId != null && !expectedTargetModelId.equals(prevTargetId)) {
                        throw error(Status.FAILED_PRECONDITION, "Existing target model " + prevTargetId
                                                                + " does not match expected " + expectedTargetModelId);
                    }
                    vmr.setTargetModel(targetModelId);
                    if (prevTargetId != null && !prevTargetId.equals(prevActiveId)) {
                        prevTargetIdToDelete = prevTargetId;
                    }
                } else {
                    // current target already matches provided target
                    if (activeMatches) {
                        break; // no record updates needed
                    }
                    if (!force) {
                        if (prevActiveId != null) {
                            prevActive = registry.getOrStrongIfAbsent(prevActiveId);
                            targetCopyCount = prevActive == null ? 0 : prevActive.getInstanceIds().size();
                        }
                        break; // no record updates needed
                    }
                }

                if (!activeMatches) {
                    // here the target model id differs from the currently active
                    // model id (a transition is required)
                    if (prevActiveId == null) {
                        vmr.setActiveModel(targetModelId);
                    } else {
                        prevActive = registry.getOrStrongIfAbsent(prevActiveId);
                        targetCopyCount = prevActive == null ? 0 : prevActive.getInstanceIds().size();
                        if (force || targetCopyCount == 0 ||
                            //TODO also count > 1
                                (targetModel != null && targetCopyCount == 1 && modelMesh.getStatus(targetModelId)
                                        .getStatus() == com.ibm.watson.modelmesh.thrift.Status.LOADED)) {
                            // we can switch over immediately if force is specified
                            // or the current active model itself is not loaded
                            vmr.setActiveModel(targetModelId);
                            prevActiveIdToDelete = prevActiveId;
                        }
                    }
                }
                vmr.setTargetLoadFailed(false);
            } else {
                // vmodel doesn't currently exist
                if (updateOnly) {
                    throw error(Status.NOT_FOUND, "vmodel " + vmid + " not found");
                }
                if (expectedTargetModelId != null && !expectedTargetModelId.equals(targetModelId)) {
                    throw error(Status.FAILED_PRECONDITION, "vmodel " + vmid + " not found");
                }
                vmr = new VModelRecord(targetModelId, owner != null ? owner : DEFAULT_OWNER);
                weCreatedVModel = true;
            }

            if (modelMesh.readOnlyMode) {
                logger.warn("Rejecting " + (weCreatedVModel ? "add" : "update") + " of VModel " + vmid
                            + " while in read-only mode");
                throw error(Status.UNAVAILABLE, "model-mesh is in read-only mode");
            }

            if (modelInfo != null && targetModel == null) {
                // we're creating a new target model
                targetModel = new ModelRecord(type, key, path, autoDelete);
                weCreatedTargetModel = true;
            }

            // update lastUsed time and ref count of target model
            long newLastUsedTime = prevActive != null && prevActive.getLastUsed() > 0L ? prevActive.getLastUsed()
                    : initialCacheTimestamp;
            targetModel.setLastUsed(newLastUsedTime);
            targetModel.incRefCount();
            // build transaction
            TableTxn txn = kvHelper.txn().set(vModels, vmid, vmr).set(registry, targetModelId, targetModel);
            if (prevTargetIdToDelete != null) {
                ModelRecord mr = registry.getOrStrongIfAbsent(prevTargetIdToDelete);
                if (decModelRefsInTxn(mr, prevTargetIdToDelete, txn)) {
                    autoDeleted.add(prevTargetIdToDelete);
                }
            }
            if (prevActiveIdToDelete != null) {
                if (decModelRefsInTxn(prevActive, prevActiveIdToDelete, txn)) {
                    autoDeleted.add(prevActiveIdToDelete);
                }
            }
            // execute transaction
            List<KVRecord> results = txn.executeAndGet();
            if (results == null) { // success
                if (weCreatedVModel) {
                    logger.info("Added new VModel " + vmid + " pointing to model " + targetModelId);
                } else {
                    if (vmr.inTransition()) {
                        logger.info("VModel " + vmid + " target is now " + targetModelId);
                    } else {
                        logger.info("VModel " + vmid + " active model changed to " + targetModelId);
                    }
                    if (!autoDeleted.isEmpty()) {
                        logger.info("Models auto-deleted via update to VModel " + vmid + ": " + autoDeleted);
                    }
                }
                break;
            }
            // transaction failed, retry with new values
            vmr = (VModelRecord) results.get(0);
            targetModel = (ModelRecord) results.get(1);
            autoDeleted.clear();
        }

        int count = targetCopyCount > 0 ? targetCopyCount : (loadNow ? 1 : 0);

        // note that in the add case, sync means wait until loaded,
        // in the set-target case, it means wait until transition is complete

        boolean inTransition = !weCreatedVModel && vmr.inTransition();

        boolean mightWait = sync && (inTransition || modelInfo != null);
        InterruptingListener cancelListener = mightWait ? GrpcSupport.newInterruptingListener() : null;
        StatusInfo curStatus;
        ModelStatusInfo targetStatus = null;
        try {
            if (!inTransition) {
                // only one concrete modelId is in play
                if (count > 0) {
                    try {
                        ModelMeshApi.setUnbalancedLitelinksContextParam(BALANCED_CTX_KEY.get());

                        //TODO include count
                        curStatus = modelMesh.ensureLoaded(targetModelId, initialCacheTimestamp, null,
                                modelInfo != null && sync, true);
                    } catch (ModelNotFoundException mle) {
                        curStatus = ModelMesh.SI_NOT_FOUND; // this would be an unlikely occurrence
                    }
                } else {
                    curStatus = weCreatedTargetModel ? ModelMesh.SI_NOT_LOADED : modelMesh.getStatus(targetModelId);
                }
                if (curStatus != null && curStatus.getStatus() == NOT_FOUND) {
                    logger.warn("updateVModel returning NOT_FOUND for model " + targetModelId
                                + " (must have been concurrently deleted)");
                }
            } else {
                // this is a set-target request that resulted in target != active
                if (sync) {
                    // wait for transition to complete
                    vmr = waitForTransition(vmid, targetModelId, 4L,
                            TimeUnit.MINUTES); //TODO base this time on model-load timeout probably
                }
                curStatus = modelMesh.getStatus(vmr.getActiveModel());
                targetStatus = resolveTargetModelStatusInfo(vmr);
            }
        } finally {
            ModelMeshApi.closeQuietly(cancelListener);
        }

        return statusFromRecord(vmr, ModelMeshApi.convertStatusInfo(curStatus), targetStatus);
    }

    protected static StatusRuntimeException error(Status status, String description) {
        throw status.withDescription(description).asRuntimeException();
    }

    // target only
    // target ! exist => error
    // vmexists
    // target != existing target => error [TBC]
    // else ok, nothing to do (return status) [then load+sync]
    // else
    // create vmid with active==target, increment target refcnt [then load+sync]
    // COND[ newVMID, TARGET ]

    // target + info
    // target exist => verify info matches
    // vmexists
    // target != existing target => error [TBC]
    // else ok, nothing to do (return status) [then load+sync]
    // else
    // target exists
    // create vmid with active==target, increment target refcnt [then load+sync]
    // COND[ newVMID, TARGET ]
    // else
    // create target w info and refcnt=1, create vmid with active==target [then load+sync]
    // COND[ newVMID, newTARGET ]

    /**
     *
     */
    public void deleteVModel(String vmid, String owner) throws Exception {
        owner = emptyToNull(owner);
        VModelRecord vmr = vModels.getOrStrongIfAbsent(vmid);
        if (absentOrOwnerMismatch(vmr, owner)) {
            return; // all good
        }

        if (modelMesh.readOnlyMode) {
            logger.warn("Rejecting deleteVModel for VModel " + vmid + " while in read-only mode");
            throw error(Status.UNAVAILABLE, "model-mesh is in read-only mode");
        }

        String active = vmr.getActiveModel(), target = vmr.getTargetModel();
        boolean modelsMatch = Objects.equals(target, active);

        ModelRecord amr = active == null ? null : registry.getOrStrongIfAbsent(active);
        ModelRecord tmr = target == null || modelsMatch ? null : registry.getOrStrongIfAbsent(target);

        List<String> autoDeleted = new ArrayList<>(2);
        while (true) {
            // build delete transaction
            TableTxn txn = kvHelper.txn();
            if (decModelRefsInTxn(amr, active, txn)) {
                autoDeleted.add(active);
            }
            if (!modelsMatch && decModelRefsInTxn(tmr, target, txn)) {
                autoDeleted.add(target);
            }

            // execute transaction - will contain between 1 and 3 components
            List<KVRecord> results = txn.delete(vModels, vmid, vmr).executeAndGet();
            if (results == null) { // success
                logger.info("VModel " + vmid + " deleted");
                if (!autoDeleted.isEmpty()) {
                    logger.info("Models auto-deleted via deletion of VModel " + vmid + ": " + autoDeleted);
                }
                return;
            }

            // if failed, process results and retry if applicable
            vmr = (VModelRecord) results.get(results.size() - 1);
            if (absentOrOwnerMismatch(vmr, owner)) {
                return; // now gone
            }

            // now refresh model details, using already-returned results where possible
            String newActive = vmr.getActiveModel(), newTarget = vmr.getTargetModel();
            modelsMatch = Objects.equals(newTarget, newActive);
            amr = updateDeleteTargetAfterTxnFail(results, newActive, active, target);
            tmr = modelsMatch ? null : updateDeleteTargetAfterTxnFail(results, newTarget, active, target);
            active = newActive;
            target = newTarget;
            autoDeleted.clear();
        }
    }

    /**
     * @return true if this will be an auto-deletion
     */
    private boolean decModelRefsInTxn(ModelRecord mr, String modelId, TableTxn txn) {
        if (mr != null) {
            // if model exists, decrement its refcount and delete if appropriate
            if (mr.decRefCount() == 0 && mr.isAutoDelete()) {
                txn.delete(registry, modelId, mr);
                return true;
            }
            txn.set(registry, modelId, mr);
        }
        // otherwise ensure its absence for the purpose of the transaction
        else if (modelId != null) {
            txn.ensureAbsent(registry, modelId);
        }
        return false;
    }

    private ModelRecord updateDeleteTargetAfterTxnFail(List<KVRecord> results, String newModelId, String priorActive,
            String priorTarget) throws Exception {
        if (newModelId == null) {
            return null;
        }
        if (newModelId.equals(priorActive)) {
            return (ModelRecord) results.get(0);
        }
        if (newModelId.equals(priorTarget)) {
            return (ModelRecord) results.get(priorActive == null ? 0 : 1);
        }
        return registry.getOrStrongIfAbsent(newModelId);
    }

    public VModelStatusInfo getVModelStatus(String vmid, String owner) throws Exception {
        owner = emptyToNull(owner);
        VModelRecord vmr = vModels.getOrStrongIfAbsent(vmid);
        if (absentOrOwnerMismatch(vmr, owner)) {
            return VModelStatusInfo.getDefaultInstance();
        }
        String modelId = vmr.getActiveModel();
        try (InterruptingListener cancelListener = GrpcSupport.newInterruptingListener()) {
            StatusInfo si = modelMesh.getStatus(modelId);
            if (si.getStatus() == NOT_FOUND) {
                vmr = vModels.getStrong(vmid);
                if (absentOrOwnerMismatch(vmr, owner)) {
                    return VModelStatusInfo.getDefaultInstance();
                }
                if (!Objects.equals(modelId, vmr.getActiveModel())) {
                    si = modelMesh.getStatus(vmr.getActiveModel());
                    if (si.getStatus() == NOT_FOUND) {
                        logger.warn("Active model " + vmr.getActiveModel() + " for vmodel " + vmid + " not found");
                    }
                }
            }
            return statusFromRecord(vmr, ModelMeshApi.convertStatusInfo(si), resolveTargetModelStatusInfo(vmr));
        }
    }

    private static boolean absentOrOwnerMismatch(VModelRecord vmr, String owner) {
        return vmr == null || owner != null && !Objects.equals(vmr.getOwner(), owner);
    }

    /**
     * @return null if vmodel is not in transition
     */
    private ModelStatusInfo resolveTargetModelStatusInfo(VModelRecord vmr) throws Exception {
        if (!vmr.inTransition()) {
            return null;
        }
        ModelRecord targetModel = registry.getOrStrongIfAbsent(vmr.getTargetModel());
        // Loading of vmodel targets is always triggered unless blocked by failed loads, so we set the state
        // to be either LOADING or LOADING_FAILED. If the model was LOADED, the vmodel would no longer be in transition.
        com.ibm.watson.modelmesh.thrift.Status status = vmr.isTargetLoadFailed() && targetModel.hasLoadFailure()
                && targetModel.getInstanceIds().isEmpty() ? com.ibm.watson.modelmesh.thrift.Status.LOADING_FAILED
                        : com.ibm.watson.modelmesh.thrift.Status.LOADING;
        return targetModel != null ? ModelMeshApi.convertStatusInfo(ModelMesh.makeStatusInfo(status, null, targetModel))
                : ModelStatusInfo.newBuilder().setStatus(ModelStatus.NOT_FOUND).build(); // unexpected
    }

    protected static VModelStatusInfo statusFromRecord(VModelRecord vmr, ModelStatusInfo msi, ModelStatusInfo tmsi) {
        if (vmr == null) return VModelStatusInfo.getDefaultInstance();
        VModelStatusInfo.Builder vmsib = VModelStatusInfo.newBuilder().setActiveModelId(vmr.getActiveModel())
                .setTargetModelId(vmr.getTargetModel())
                .setStatus(!vmr.inTransition() ? VModelStatus.DEFINED
                        : (!vmr.isTargetLoadFailed() ? VModelStatus.TRANSITIONING : VModelStatus.TRANSITION_FAILED))
                .setActiveModelStatus(msi);
        if (tmsi != null) vmsib.setTargetModelStatus(tmsi);
        if (vmr.getOwner() != null) vmsib.setOwner(vmr.getOwner());
        return vmsib.build();
    }

    /**
     * @param vModelId
     * @param notFoundModelId not-found modelId from prior attempt
     * @return modelId, or null modelId equals notFoundModelId
     * @throws StatusRuntimeException if vmodel isn't found
     */
    protected String resolveVModelId(String vModelId, String notFoundModelId) throws Exception {
        VModelRecord vmr = vModels.getOrStrongIfAbsent(vModelId);
        if (vmr == null) {
            throw ModelMeshApi.VMODEL_NOT_FOUND_STATUS_E;
        }
        String modelId = vmr.getActiveModel();
        if (notFoundModelId == null || !notFoundModelId.equals(modelId)) {
            return modelId;
        }
        logger.info("DEBUG: Target model " + notFoundModelId + " of vmodel " + vModelId
                    + " not found on first attempt, making strong read of vmodel rec");
        Exception getStrongException = null;
        try {
            vmr = vModels.getStrong(vModelId);
            modelId = vmr.getActiveModel();
        } catch (Exception e) {
            getStrongException = e;
        }
        if (!notFoundModelId.equals(modelId)) {
            return modelId;
        }
        modelId = vmr.getTargetModel();
        if (!notFoundModelId.equals(modelId)) {
            return modelId;
        }
        throw getStrongException != null ? getStrongException
                : GrpcSupport.asLeanException(Status.INTERNAL
                .withDescription("Target model " + notFoundModelId + " of vmodel " + vModelId + " not found"));
    }

    private static boolean isTransitionedOrFailed(VModelRecord vmr, String targetModelId) {
        return vmr == null || vmr.isTargetLoadFailed() || !targetModelId.equals(vmr.getTargetModel())
               || !vmr.inTransition();
    }

    // ------- transitioner logic, runs only in leader instance

    // modified only by serialized vModelProcExecutor and completed PendingTransition tasks themselves
    protected final Map<String, PendingTransition> pendingTransitions = new ConcurrentHashMap<>();

    // map of targetModelId -> set of vModelIds, modified only by serialized vModelProcExecutor
    protected final Map<String, Set<String>> stuckTransitions = new ConcurrentHashMap<>();

    protected final ListeningExecutorService targetScaleupExecutor = MoreExecutors.listeningDecorator(
            Executors.newCachedThreadPool(ThreadPoolHelper.threadFactory("mm-vmodel-trans-thread-%d")));

    protected final Executor vModelProcExecutor = GrpcClient.serialized(targetScaleupExecutor);

    void processVModels() {
        if (modelMesh.readOnlyMode) {
            return; // don't process any VModel transitions in read-only mode
        }

        vModelProcExecutor.execute(() -> {
            // Clear map of stuck transitions since we are about to process all vmodels. Any transitions
            // still stuck will be repopulated.
            stuckTransitions.clear();
            try {
                for (Entry<String, VModelRecord> ent : vModels) {
                    processVModel(ent.getKey(), ent.getValue());
                }
            } catch (Exception e) {
                logger.error("Error processing vmodels", e);
            }
        });
    }

    /**
     * Ensures a PendingTransition task is running if necessary; run only in leader instance
     */
    private void processVModel(String vmid, VModelRecord vmr) {
        // don't process any VModel transitions in read-only mode
        if (modelMesh.readOnlyMode || vmr == null || !vmr.inTransition()) {
            return;
        }
        PendingTransition pt = pendingTransitions.get(vmid);
        if (pt != null) {
            if (Objects.equals(pt.fromId, vmr.getActiveModel()) && Objects.equals(pt.toId, vmr.getTargetModel())) {
                return; // nothing to do
            }
            // current and/or target changed
            pendingTransitions.remove(vmid, pt);
            pt.ensureLoadedTask.cancel(true);
        }
        pt = new PendingTransition(vmid, vmr);
        pendingTransitions.put(vmid, pt);
        boolean submitted = false;
        try {
            pt.ensureLoadedTask = targetScaleupExecutor.submit(pt);
            submitted = true;
        } finally {
            if (!submitted) {
                pendingTransitions.remove(vmid, pt);
            }
        }
    }

    void processModelChange(String modelId) {
        if (modelMesh.readOnlyMode) {
            return;
        }
        // If this model was holding up a vModel transition, attempt to start
        // a pending transition thread again (to re-initiate loading if possible)
        Set<String> vmrs = stuckTransitions.get(modelId);
        if (vmrs != null) {
            vModelProcExecutor.execute(() -> {
                Set<String> vmrs2 = stuckTransitions.remove(modelId);
                if (vmrs2 != null) {
                    for (String vmid : vmrs2) {
                        processVModel(vmid, vModels.get(vmid));
                    }
                }
            });
        }
    }

    class PendingTransition implements Runnable {
        final String vmid;
        final VModelRecord origVmr;
        final String fromId, toId;
        ListenableFuture<?> ensureLoadedTask;

        public PendingTransition(String vmid, VModelRecord vmr) {
            this.vmid = vmid;
            this.origVmr = vmr;
            this.fromId = vmr.getActiveModel();
            this.toId = vmr.getTargetModel();
        }

        @Override
        public void run() {
            try {
                ModelRecord curActive = registry.getOrStrongIfAbsent(fromId);
                int targetCount = curActive == null ? 0 : curActive.getInstanceIds().size();
                if (targetCount > 0) {
                    //TODO later support bigger scale [use match-scale version of ensureLoaded]
                    long lastUsed = curActive.getLastUsed();
                    if (lastUsed == 0) {
                        lastUsed = currentTimeMillis() - ModelMesh.LASTUSED_AGE_ON_ADD_MS;
                    }

                    // this will block until loaded
                    StatusInfo si = modelMesh.ensureLoadedInternal(toId, lastUsed, 0, null, 0, true);
                    if (si == null) {
                        return; // shouldn't happen
                    }
                    boolean targetLoadFailed = false;
                    switch (si.getStatus()) {
                    case NOT_CHECKED:
                    case NOT_FOUND:
                    default:
                        return;
                    case LOADING:
                    case LOADING_FAILED:
                        targetLoadFailed = true;
                    case NOT_LOADED:
                        VModelRecord vmrec = vModels.get(vmid);
                        if (vmrec != null) {
                            vModelProcExecutor.execute(() -> {
                                // Add to stuckTransisions map so that loading will be retried immediately
                                // once failures expire.
                                Set<String> vmrs = stuckTransitions.get(toId);
                                if (vmrs == null) {
                                    stuckTransitions.put(toId, vmrs = new TreeSet<>());
                                }
                                vmrs.add(vmid);
                            });
                            if (targetLoadFailed != vmrec.isTargetLoadFailed()) {
                                vmrec.setTargetLoadFailed(targetLoadFailed);
                                if (vModels.conditionalSet(vmid, vmrec)) {
                                    logger.info("Updated targetLoadFailed flag for VModel " + vmid + " to "
                                                + targetLoadFailed);
                                }
                            }
                        }
                        return;
                    case LOADED:
                        break; // continue to update below
                    }
                }
                // transition is complete, proceed to update records
                VModelRecord vmrec = origVmr;
                boolean autoDeleted;
                while (true) {
                    vmrec.setActiveModel(toId);
                    vmrec.setTargetLoadFailed(false);
                    TableTxn txn = kvHelper.txn().set(vModels, vmid, vmrec);
                    autoDeleted = decModelRefsInTxn(curActive, fromId, txn);
                    List<KVRecord> results = txn.executeAndGet();
                    if (results == null) {
                        break; // done
                    }
                    vmrec = (VModelRecord) results.get(0);
                    if (vmrec == null || !fromId.equals(vmrec.getActiveModel()) ||
                        !toId.equals(vmrec.getTargetModel())) {
                        return; // things changed
                    }
                    curActive = (ModelRecord) results.get(1);
                }
                logger.info("VModel " + vmid + " completed transition from " + fromId + " to " + toId);
                if (autoDeleted) {
                    logger.info("Model " + fromId + " auto-deleted via transtion of VModel " + vmid);
                }
            } catch (ModelNotFoundException mnfe) {
            } catch (Exception e) {
                if (!ModelMesh.isInterruption(e)) {
                    logger.error("vmodel transition task for " + vmid + " failed with exception", e);
                }
            } finally {
                pendingTransitions.remove(vmid, this);
            }
        }
    }

    // ------- waiter mechanics, used for "sync" setVModelTarget requests

    static class Waiter extends AbstractFuture<VModelRecord> {
        final String targetModelId;

        public Waiter(String targetModelId) {
            this.targetModelId = targetModelId;
        }

        @Override
        public boolean set(VModelRecord vmr) {
            return super.set(vmr);
        }
    }

    //TODO double-check that stale can't get left in here
    final Map<String, Set<Waiter>> waiters = new ConcurrentHashMap<>();

    final KVTable.Listener<VModelRecord> VMODEL_LISTENER = (event, vmid, vmr) -> {
        if (event == KVTable.EventType.INITIALIZED) {
            return;
        }
        if (event == KVTable.EventType.ENTRY_DELETED) {
            vmr = null;
        }
        Set<Waiter> subs = waiters.get(vmid);
        while (true) {
            if (subs == null) {
                return;
            }
            synchronized (subs) {
                Set<Waiter> cur = waiters.get(vmid);
                if (cur != subs) {
                    subs = cur;
                    continue;
                }
                for (Iterator<Waiter> it = subs.iterator(); it.hasNext(); ) {
                    Waiter w = it.next();
                    if (isTransitionedOrFailed(vmr, w.targetModelId)) {
                        w.set(vmr);
                        it.remove();
                    }
                }
                if (subs.isEmpty()) {
                    waiters.remove(vmid, subs);
                }
            }
            break;
        }
    };

    private VModelRecord waitForTransition(String vModelId, String targetModelId, long timeout, TimeUnit unit)
            throws TimeoutException, InterruptedException {
        Waiter waiter = new Waiter(targetModelId);
        Set<Waiter> subs = waiters.get(vModelId);
        while (true) {
            boolean weCreated = false;
            if (subs == null) {
                subs = new HashSet<>(4);
                weCreated = true;
            }
            Set<Waiter> cur;
            synchronized (subs) {
                cur = !weCreated ? waiters.get(vModelId) : waiters.putIfAbsent(vModelId, subs);
                if (cur == (weCreated ? null : subs)) {
                    VModelRecord vmr = vModels.get(vModelId);
                    if (isTransitionedOrFailed(vmr, targetModelId)) {
                        if (weCreated) {
                            waiters.remove(vModelId, subs);
                        }
                        return vmr;
                    }
                    subs.add(waiter);
                    break;
                }
            }
            subs = cur;
        }
        try {
            return waiter.get(timeout, unit);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause()); // should not be possible
        }
    }

}
