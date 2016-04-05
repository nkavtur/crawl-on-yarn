package home.nkavtur.master;

import static com.google.common.base.Joiner.on;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static com.google.common.collect.Maps.*;
import static home.nkavtur.utils.Constants.*;
import static home.nkavtur.utils.YarnUtils.*;

import static java.io.File.*;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer.*;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;
import home.nkavtur.container.CrawlAndAnalyzeApp;
import home.nkavtur.master.CrawlApplicationMaster.NMCallbackHandler;

/**
 * @author nkavtur
 */
public class ContainerRunner implements Runnable {
    private static final Logger LOG = Logger.getLogger(ContainerRunner.class);

    private Container container;
    private NMCallbackHandler nodeManagerListener;
    private Map<String, LocalResource> localResources = newHashMap();;
    private Configuration configuration = new YarnConfiguration();
    private CrawlApplicationMaster master;
    private FileSystem fs;

    public ContainerRunner(CrawlApplicationMaster simpleApplicationMaster, 
            Container container,
            NMCallbackHandler nodeManagerListener) {
        this.master = simpleApplicationMaster;
        this.container = container;
        this.nodeManagerListener = nodeManagerListener;

        try {
            this.fs = FileSystem.get(configuration);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        ContainerId containerId = container.getId();
        ApplicationId applicationId = containerId.getApplicationAttemptId().getApplicationId();
        ContainerLaunchContext containerContext = Records.newRecord(ContainerLaunchContext.class);
        containerContext.setEnvironment(Maps.<String, String> newHashMap());

        try {
            Path appJar = new Path(new URI(master.getAppJar()));
            localResources.put(APPNAME_JAR, getFileResource(appJar, fs));

            Path log4j = new Path(new URI(master.getLog4jPropFile()));
            localResources.put("log4j.properties", getFileResource(log4j, fs));
        } catch (Exception e) {
            e.printStackTrace();
        }

        containerContext.setLocalResources(localResources);

        String containerHome = configuration.get("yarn.nodemanager.local-dirs") + separator
                + USERCACHE + separator + System.getenv().get(Environment.USER.toString())
                + separator + APPCACHE + separator + applicationId + separator + containerId;

        List<String> launchCommands = newArrayList();
        launchCommands.add(Environment.JAVA_HOME.$() + "/bin/java");
        launchCommands.add("-cp " + "$(hadoop classpath):/etc/hadoop/*" + pathSeparator + containerHome + separator + APPNAME_JAR);
        launchCommands.add(CrawlAndAnalyzeApp.class.getName());
        launchCommands.add("-" + USER_PROFILE_TAGS + " " + master.getUserProfileTags());

        String launchCommand = on(" ").join(launchCommands);
        LOG.info(format("Launch command: [%s]", launchCommand));

        containerContext.setCommands(Collections.singletonList(launchCommand));

        nodeManagerListener.addContainer(container.getId(), container);
        master.getNmClientAsync().startContainerAsync(container, containerContext);
    }
}
