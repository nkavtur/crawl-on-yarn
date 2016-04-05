package home.nkavtur.utils;

import static org.apache.hadoop.yarn.api.records.LocalResourceType.FILE;
import static org.apache.hadoop.yarn.api.records.LocalResourceVisibility.APPLICATION;
import static org.apache.hadoop.yarn.util.ConverterUtils.getYarnUrlFromPath;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;

/**
 * @author nkavtur
 */
public final class YarnUtils {

    public static LocalResource getFileResource(Path dst, FileSystem fs) throws IOException {
        FileStatus amJarStatus = fs.getFileStatus(dst);
        LocalResource resource = Records.newRecord(LocalResource.class);
        resource.setSize(amJarStatus.getLen());
        resource.setTimestamp(amJarStatus.getModificationTime());
        resource.setType(FILE);
        resource.setVisibility(APPLICATION);
        resource.setResource(getYarnUrlFromPath(dst));
        return resource;
    }
}
