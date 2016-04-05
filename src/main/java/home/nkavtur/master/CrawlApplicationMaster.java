package home.nkavtur.master;

import static java.lang.Integer.parseInt;
import static home.nkavtur.utils.Constants.*;
import static java.lang.String.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/**
 * @author nkavtur
 */
public class CrawlApplicationMaster {
    private static final Logger LOG = Logger.getLogger(CrawlApplicationMaster.class);

    private Configuration configuration;

    private AMRMClientAsync<ContainerRequest> resourceManager;
    private NMClientAsync nodeManager;
    private NMCallbackHandler nodeManagerListener;

    private int containersNum;
    private int containerMemory;
    private int priority;
    private int containerCores;

    private AtomicInteger numCompletedContainers = new AtomicInteger();
    private AtomicInteger numAllocatedContainers = new AtomicInteger();
    private AtomicInteger numFailedContainers = new AtomicInteger();
    private AtomicInteger numRequestedContainers = new AtomicInteger();

    private String appJar;
    private String userProfileTags;
    private String log4jPropFile;

    private volatile boolean done;
    private Options options;
    private List<Thread> launchThreads = Lists.newArrayList();

    public static void main(String[] args) {
        try {
            CrawlApplicationMaster appMaster = new CrawlApplicationMaster();
            appMaster.init(args);
            appMaster.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CrawlApplicationMaster() throws Exception {
        configuration = new YarnConfiguration();
        options = new Options();
        options.addOption(CONTAINER_MEMORY, true, "Amount of memory in MB to be requested to run the shell command");
        options.addOption(CONTAINER_NUM, true, "Amount of containers");
        options.addOption(JAR, true, "JAR file containing the application");
        options.addOption(PRIORITY, true, "Application Priority. Default 0");
        options.addOption(CONTAINER_CORES, true, "Amount of virtual cores to be requested to run the shell command");
        options.addOption(USER_PROFILE_TAGS, true, "User profile tags file");
        options.addOption(LOG_PROPERTIES, true, "log4j.properties file");
        options.addOption(HELP, false, "Print usage");

    }

    public void init(String[] args) throws ParseException, IOException {
        CommandLine commandLine = new GnuParser().parse(options, args);
        containerMemory = parseInt(commandLine.getOptionValue(CONTAINER_MEMORY, DEFAULT_MEMORY));
        containersNum = parseInt(commandLine.getOptionValue(CONTAINER_NUM, DEFAULT_CONTAINER_NUM));
        containerCores = parseInt(commandLine.getOptionValue(CONTAINER_CORES, DEFAULT_CORES));
        appJar = commandLine.getOptionValue(JAR);
        userProfileTags = commandLine.getOptionValue(USER_PROFILE_TAGS);
        priority = parseInt(commandLine.getOptionValue(PRIORITY, DEFAULT_PRIORITY));
        log4jPropFile = commandLine.getOptionValue(LOG_PROPERTIES);
        if (commandLine.hasOption(HELP)) printHelp();
    }

    private void printHelp() {
        new HelpFormatter().printHelp(CrawlApplicationMaster.class.getSimpleName(), options);
    }

    public void run() throws Exception {
        LOG.info("Starting resource manager client");
        AMRMClientAsync.CallbackHandler resourceManagerListener = new RMCallbackHandler();
        resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, resourceManagerListener);
        resourceManager.init(configuration);
        resourceManager.start();

        LOG.info("Starting node manager client");
        nodeManagerListener = new NMCallbackHandler();
        nodeManager = new NMClientAsyncImpl(nodeManagerListener);
        nodeManager.init(configuration);
        nodeManager.start();

        resourceManager.registerApplicationMaster("", 0, "");
        for (int i = 0; i < containersNum; i++) {
            ContainerRequest containerAsk = setupContainerAskForRM();
            LOG.info(format("Registrating container request for resource manager: [%s]", containerAsk));
            resourceManager.addContainerRequest(containerAsk);
        }
        numRequestedContainers.set(containersNum);

        while (!done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
            }
        }
        finish();
    }

    private void finish() throws Exception {
        for (Thread launchThread : launchThreads) {
            launchThread.join(10000);
        }

        LOG.info("Stopping node managers client");
        nodeManager.stop();
        FinalApplicationStatus appStatus = numFailedContainers.get() == 0 && numCompletedContainers.get() == containersNum
                ? FinalApplicationStatus.SUCCEEDED : FinalApplicationStatus.FAILED;

        LOG.info(format("Stopping resource manager client with filalSatus=[%s]", appStatus));
        resourceManager.unregisterApplicationMaster(appStatus, null, null);
        done = true;
        resourceManager.stop();
    }

    private ContainerRequest setupContainerAskForRM() {
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(priority);
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory);
        capability.setVirtualCores(containerCores);
        ContainerRequest request = new ContainerRequest(capability, null, null, pri);
        return request;
    }

    /**
     * @author mkautur
     */
    class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            for (ContainerStatus containerStatus : completedContainers) {
                int exitStatus = containerStatus.getExitStatus();
                LOG.info(format("ContainerId=[%s], exitStatus=[%s], status=[%s], diagnostics=[%s]",
                        containerStatus.getContainerId(), exitStatus, 
                        containerStatus.getState(), containerStatus.getDiagnostics()));

                if (0 != exitStatus) {
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                    }
                } else {
                    numCompletedContainers.incrementAndGet();
                }
            }

            if (numCompletedContainers.get() == containersNum) {
                done = true;
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                LOG.info(format("Container id=[%s] was allocated", allocatedContainer.getId()));
                ContainerRunner runnableLaunchContainer = new ContainerRunner(
                    CrawlApplicationMaster.this, allocatedContainer, nodeManagerListener);
                Thread launchThread = new Thread(runnableLaunchContainer);
                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        @Override
        public void onShutdownRequest() {
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        @Override
        public float getProgress() {
            return (float) numCompletedContainers.get() / containersNum;
        }

        @Override
        public void onError(Throwable e) {
            done = true;
            resourceManager.stop();
        }
    }

    /**
     * @author mkautur
     */
    class NMCallbackHandler implements NMClientAsync.CallbackHandler {
        private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();

        public void addContainer(ContainerId containerId, Container container) {
            LOG.info(format("Container id=[%s] was added", containerId));
            containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            LOG.info(format("Container id=[%s] was stopped", containerId));
            containers.remove(containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
            LOG.info(format("Container id=[%s] status=[%s]", containerId, containerStatus));
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
            Container container = containers.get(containerId);
            LOG.info(format("Container id=[%s] started", containerId));
            if (container != null) {
                nodeManager.getContainerStatusAsync(containerId, container.getNodeId());
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.info(format("Container id=[%s] started with error=[%s]", containerId, t));
            containers.remove(containerId);
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {}

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.info(format("Container id=[%s] stopped with error=[%s]", containerId, t));
            containers.remove(containerId);
        }

        public int getContainerCount() {
            return containers.size();
        }
    }

    public String getAppJar() {
        return appJar;
    }

    public void setAppJar(String appJar) {
        this.appJar = appJar;
    }

    public NMClientAsync getNmClientAsync() {
        return nodeManager;
    }

    public void setNmClientAsync(NMClientAsync nmClientAsync) {
        this.nodeManager = nmClientAsync;
    }

    public String getUserProfileTags() {
        return userProfileTags;
    }

    public String getLog4jPropFile() {
        return log4jPropFile;
    }
}