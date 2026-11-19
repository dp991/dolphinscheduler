package org.apache.dolphinscheduler.plugin.task.sparkoperator;

import org.apache.dolphinscheduler.plugin.task.api.AbstractRemoteTask;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Custom DolphinScheduler remote task plugin that submits and tracks
 * sparkoperator.k8s.io/v1beta2 SparkApplication CRDs via Fabric8,
 * injecting tenant-scoped Kubernetes PodTemplateSpec overlays at submission time.
 */
public abstract class SparkOperatorTask extends AbstractRemoteTask {

    private final TaskExecutionContext taskExecutionContext;
    private String sparkAppName;

    protected SparkOperatorTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;
    }

    @Override
    public void init() {
        this.sparkAppName = String.format(
                "ds-spark-%d-%d",
                taskExecutionContext.getProcessInstanceId(),
                taskExecutionContext.getTaskInstanceId());
    }

    /**
     * Compiles task parameters into a sparkoperator.k8s.io/v1beta2 SparkApplication CR
     * and merges tenant-specific driver/executor PodTemplateSpec overlays (secrets,
     * tolerations, PVCs, sidecars) before applying through Fabric8 KubernetesClient.
     */
    protected abstract Map<String, Object> buildSparkApplicationCr(String namespace, String tenantCode);

    @Override
    public void submitApplication() throws TaskException {
        Map<String, Object> crPayload = buildSparkApplicationCr(
                taskExecutionContext.getK8sTaskExecutionContext().getNamespace(),
                taskExecutionContext.getTenantCode());
        // Fabric8 GenericKubernetesResource submission handled in concrete implementation
        if (crPayload == null || crPayload.isEmpty()) {
            throw new TaskException("Failed to build SparkApplication CRD payload for " + sparkAppName);
        }
    }

    @Override
    public void trackApplicationStatus() throws TaskException {
        // Poll .status.applicationState.state via Fabric8 watch/informer
    }

    @Override
    public void cancelApplication() throws TaskException {
        // Delete SparkApplication CR cleanly on task kill or timeout to prevent orphan pods
    }

    @Override
    public List<String> getApplicationIds() throws TaskException {
        return sparkAppName == null ? Collections.emptyList() : Collections.singletonList(sparkAppName);
    }

    @Override
    public AbstractParameters getParameters() {
        return null;
    }
}
