package home.nkavtur.client;

import static com.google.common.base.Joiner.on;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang.StringUtils.EMPTY;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.hadoop.yarn.api.records.LocalResourceType.FILE;
import static org.apache.hadoop.yarn.api.records.LocalResourceVisibility.APPLICATION;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.YARN_APPLICATION_CLASSPATH;
import static org.apache.hadoop.yarn.util.ConverterUtils.getYarnUrlFromPath;
import static home.nkavtur.utils.Constants.*;
import static org.apache.hadoop.yarn.util.Records.*;
import static java.io.File.*;
import org.apache.hadoop.yarn.util.Records;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.Logger;

import home.nkavtur.master.CrawlApplicationMaster;

/**
 * @author nkavtur
 */
public class CrawlAppClient {
    private static final Logger LOG = Logger.getLogger(CrawlAppClient.class);

    private Configuration configuration =  new YarnConfiguration();
    private YarnClient yarnClient;
    private FileSystem fs;
    private Map<String, LocalResource> localResources = newHashMap();;

    private int priority;
    private String queue;
    private int masterMemory;
    private int masterCores;
    private String hdfsAppUri;
    private String masterMainClass = CrawlApplicationMaster.class.getName();
    private int containerMemory;
    private int containerCores;
    private int containersNumber;
    private String log4jPropFile;
    private String userProfileTags;
    private String hdfsUserProfileTagsUri;
    private String hdfsLog4jUri;
    private Options options;

    public CrawlAppClient(String[] args) throws IOException {
        fs = FileSystem.get(configuration);
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);

        options = new Options();
        options.addOption(PRIORITY, true, "Application Priority. Default 0");
        options.addOption(QUEUE, true, "RM Queue in which this application is to be submitted");
        options.addOption(MASTER_MEMORY, true, "Amount of memory in MB to be requested to run the application master");
        options.addOption(MASTER_CORES, true, "Amount of virtual cores to be requested to run the application master");
        options.addOption(CONTAINER_MEMORY, true, "Amount of memory in MB to be requested to run the container");
        options.addOption(CONTAINER_CORES, true, "Amount of virtual cores to be requested to run the container");
        options.addOption(CONTAINER_NUM, true, "Amount of containers to be requested");
        options.addOption(LOG_PROPERTIES, true, "log4j.properties file");
        options.addOption(USER_PROFILE_TAGS, true, "User profile tags file");
        options.addOption(HELP, false, "Print help");
    }

    public static void main(String[] args) {
        try {
            CrawlAppClient client = new CrawlAppClient(args);
            client.init(args);
            client.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init(String[] args) throws ParseException {
        CommandLine commandLine = new GnuParser().parse(options, args);
        priority = parseInt(commandLine.getOptionValue(PRIORITY, DEFAULT_PRIORITY));
        queue = commandLine.getOptionValue(QUEUE, DEFAULT_QUEUE);
        masterMemory = parseInt(commandLine.getOptionValue(MASTER_MEMORY, DEFAULT_MEMORY));
        masterCores = parseInt(commandLine.getOptionValue(MASTER_CORES, DEFAULT_CORES));
        containerMemory = parseInt(commandLine.getOptionValue(CONTAINER_MEMORY, DEFAULT_MEMORY));
        containerCores = parseInt(commandLine.getOptionValue(CONTAINER_CORES, DEFAULT_CORES));
        containersNumber = parseInt(commandLine.getOptionValue(CONTAINER_NUM, DEFAULT_CONTAINER_NUM));
        log4jPropFile = commandLine.getOptionValue(LOG_PROPERTIES, EMPTY);
        userProfileTags = commandLine.getOptionValue(USER_PROFILE_TAGS);
        if (isBlank(userProfileTags)) {
            printHelp();
            throw new IllegalArgumentException("Please, specify userProfileTags file location.");
        }
        
        if (commandLine.hasOption(HELP)) printHelp();
    }

    private void run() throws YarnException, IOException, InterruptedException {
        LOG.info("Starting yarn client");
        yarnClient.start();

        YarnClientApplication application = yarnClient.createApplication();
        ApplicationSubmissionContext submissionContext = application.getApplicationSubmissionContext();
        submissionContext.setApplicationName(APPNAME);
        ApplicationId appId = submissionContext.getApplicationId();
        LOG.info(format("Got appId=[%s] from submissionContext", appId));

        ContainerLaunchContext applicationManager = newRecord(ContainerLaunchContext.class);

        Path appDst = copyToHDFS(getCurrentJarPath(), APPNAME + separator + appId.getId() + separator + APPNAME_JAR);
        LOG.info(format("Application jar was copied to [%s]", appDst));
        localResources.put(APPNAME_JAR, getFileResource(appDst, APPLICATION));
        hdfsAppUri = appDst.toUri().toString();

        Path userProfileTagsDst = copyToHDFS(userProfileTags, APPNAME + separator + appId.getId() + separator + "user.profile.tags.us.txt");
        LOG.info(format("UserProfileTags file was copied to [%s]", userProfileTagsDst));
        localResources.put(USER_PROFILE_TAGS, getFileResource(userProfileTagsDst, APPLICATION));
        hdfsUserProfileTagsUri = userProfileTagsDst.toUri().toString();

        if (isNotBlank(log4jPropFile)) {
            Path log4jDst = copyToHDFS(log4jPropFile, "log4j.props");
            LOG.info(format("Log4j properties was copied to [%s]", log4jDst));
            localResources.put("log4j.properties", getFileResource(log4jDst, APPLICATION));
            hdfsLog4jUri = log4jDst.toUri().toString();
        }

        applicationManager.setEnvironment(getEnvironment());
        applicationManager.setCommands(singletonList(getLaunchMasterCommand()));
        submissionContext.setResource(getResourceCapability());
        submissionContext.setAMContainerSpec(applicationManager);
        submissionContext.setPriority(getPriority());
        submissionContext.setQueue(queue);

        LOG.info("Submitting application");
        yarnClient.submitApplication(submissionContext);

        LOG.info("Started monitoring application");
        monitorApplication(appId);
    }

    private void printHelp() {
        new HelpFormatter().printHelp(CrawlAppClient.class.getName(), options);
    }

    private String getCurrentJarPath() {
        return this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
    }

    private Path copyToHDFS(String src, String dst) throws IOException {
        Path srcPath = new Path(src);
        Path dstPath = new Path(fs.getHomeDirectory(), dst);
        fs.copyFromLocalFile(false, true, srcPath, dstPath);
        return dstPath;
    }

    private LocalResource getFileResource(Path dst, LocalResourceVisibility visibility) throws IOException {
        FileStatus amJarStatus = fs.getFileStatus(dst);
        LocalResource resource = Records.newRecord(LocalResource.class);
        resource.setSize(amJarStatus.getLen());
        resource.setTimestamp(amJarStatus.getModificationTime());
        resource.setType(FILE);
        resource.setVisibility(visibility);
        resource.setResource(getYarnUrlFromPath(dst));
        return resource;
    }

    private Map<String, String> getEnvironment() {
        Map<String, String> environment = newHashMap();
        String classpath = Environment.CLASSPATH.$() + pathSeparator + "./*jar";
        for (String c : configuration.getStrings(YARN_APPLICATION_CLASSPATH, DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classpath += pathSeparatorChar + c.trim();
        }
        classpath += pathSeparatorChar + "./log4j.properties";
        classpath += System.getProperty("java.class.path");
        environment.put("CLASSPATH", classpath);
        LOG.info(format("Passing environment: [%s]", environment));

        return environment;
    }

    private String getLaunchMasterCommand() {
        List<String> launchCommands = newArrayList();
        launchCommands.add(Environment.JAVA_HOME.$() + "/bin/java");
        launchCommands.add("-Xmx" + masterMemory + "m");
        launchCommands.add(masterMainClass);
        launchCommands.add("--" + CONTAINER_MEMORY + " " + containerMemory);
        launchCommands.add("--" + CONTAINER_CORES + " " + containerCores);
        launchCommands.add("--" + CONTAINER_NUM + " " + containersNumber);
        launchCommands.add("--" + PRIORITY + " " + priority);
        launchCommands.add("--" + JAR + " " + hdfsAppUri);
        launchCommands.add("--" + USER_PROFILE_TAGS + " " + hdfsUserProfileTagsUri);
        if (isNotBlank(hdfsLog4jUri)) {
            launchCommands.add("--" + LOG_PROPERTIES + " " + hdfsLog4jUri);
        }
        launchCommands.add("1>" + "/var/log/hadoop/yarn/" + CrawlApplicationMaster.class.getSimpleName() + ".stdout");
        launchCommands.add("2>" + "/var/log/hadoop/yarn/" + CrawlApplicationMaster.class.getSimpleName() + ".stderr");

        String launchCommand = on(" ").join(launchCommands);
        LOG.info(format("Launch command: [%s]", launchCommand));
        return launchCommand;
    }

    private Resource getResourceCapability() {
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(masterMemory);
        capability.setVirtualCores(masterCores);
        LOG.info(format("Resource capability: [%s]", capability));
        return capability;
    }

    private Priority getPriority() {
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(this.priority);
        LOG.info(format("Priority: [%s]", priority));
        return priority;
    }

    private void monitorApplication(ApplicationId appId) throws InterruptedException, YarnException, IOException {
        while (true) {
            Thread.sleep(1000);
            ApplicationReport report = yarnClient.getApplicationReport(appId);
            LOG.info("appId=[" + appId.getId() + "], " + "clientToAMToken=[" + report.getClientToAMToken() + "], "
                    + "appDiagnostics=[" + report.getDiagnostics() + "], " + "appMasterHost=[" + report.getHost()
                    + "], " + "appQueue=[" + report.getQueue() + "], " + "appMasterRpcPort=[" + report.getRpcPort()
                    + "], " + "appStartTime=[" + report.getStartTime() + "], " + "yarnAppState=["
                    + report.getYarnApplicationState().toString() + "], " + "distributedFinalState=["
                    + report.getFinalApplicationStatus().toString() + "], " + "appTrackingUrl=["
                    + report.getTrackingUrl() + "], " + "appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus status = report.getFinalApplicationStatus();
            if (state == YarnApplicationState.FINISHED || state == YarnApplicationState.KILLED
                    || state == YarnApplicationState.FAILED) {
                LOG.info("Yarn state=[" + state + "], " + "job status=[" + status + "]");
            }
        }
    }
}
