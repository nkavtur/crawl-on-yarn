package home.nkavtur.container;

import static home.nkavtur.utils.Constants.USER_PROFILE_TAGS;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import home.nkavtur.utils.CsvConverter;
import home.nkavtur.utils.UserProfileTagsEntry;

/**
 * @author nkavtur
 */
public class CrawlAndAnalyzeApp {
    private static final Logger LOG = Logger.getLogger(CrawlAndAnalyzeApp.class);
    private static final String WORD_REGEXP = "[\\s]+";
    private static final Pattern TAG_REGEXP_PATTERN = Pattern.compile("\\<.*?>");

    private String userProfileTagsFile;
    private Options options;
    private FileSystem fs;
    private Configuration configuration = new YarnConfiguration();

    public CrawlAndAnalyzeApp() throws IOException {
        fs = FileSystem.get(configuration);
        options = new Options();
        options.addOption(USER_PROFILE_TAGS, true, "User profile tags file");
    }

    public static void main(String[] args) throws IOException {
        try {
            CrawlAndAnalyzeApp app = new CrawlAndAnalyzeApp();
            app.init(args);
            app.run();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private void init(String[] args) throws ParseException {
        CommandLine commandLine = new GnuParser().parse(options, args);
        userProfileTagsFile = commandLine.getOptionValue(USER_PROFILE_TAGS);
    }

    private void run() throws IOException { 
        LOG.info("Create list of user profile entries with parsed html");
        List<UserProfileTagsEntry> userProfileTags = getUserProfuleTagEnries();

        LOG.info("select top 10 words foreach userProfileTagEntry");
        userProfileTags.forEach(userProfileTag -> 
            userProfileTag.withValue(getTop10Words(userProfileTag)));

        Path dst = new Path(new Path(this.userProfileTagsFile).getParent() + File.separator + "res.csv");
        LOG.info(format("Creating result file on path=[%s]", dst));
        writeResultToHdfs(dst, CsvConverter.toCsv(userProfileTags));
    }

    private List<UserProfileTagsEntry> getUserProfuleTagEnries() throws IOException {
        return getReader().lines().
                skip(1). // Skip Header
                map((String line) -> {
                    String[] words = line.split(WORD_REGEXP);
                    return new UserProfileTagsEntry().withId(words[0]).withStatus(words[1]).withPricingType(words[3])
                            .withMatchType(words[3]).withUrl(words[4]);
                }).
                peek(userProfileTag -> userProfileTag.withHtml(parseHtml(userProfileTag.getUrl()))).
                collect(toList());
    }

    private BufferedReader getReader() throws IllegalArgumentException, IOException {
        InputStream input = fs.open(new Path(userProfileTagsFile));
        return new BufferedReader(new InputStreamReader(input));
    }

    private List<String> getTop10Words(UserProfileTagsEntry userProfileTag) {
        List<String> top10 = Stream.of(userProfileTag.getHtml().split(WORD_REGEXP)).
                map(String::toLowerCase).
                collect(groupingBy(identity(), counting())).
                entrySet().stream().
                sorted((a, b) -> (int) Math.signum(b.getValue() - a.getValue())).
                limit(10).
                map(Entry::getKey).
                collect(toList());
        LOG.info(format("Top 10 words for url=[%s] are [%s]", userProfileTag.getUrl(), top10));
        return top10;
    }

    private String parseHtml(String url) {
        try {
            HttpClient client = new DefaultHttpClient();
            HttpGet request = new HttpGet(url);
            HttpResponse response = client.execute(request);

            String content = IOUtils.toString(response.getEntity().getContent());
            String text = parse(content);

            // Noticed, an interesting thing, that Jsoup library can't always parse the whole DOM f first attemt
            // this simple workaround helps to parse DOM fully
            return stillContainsTags(text) ? parse(text) : text;
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    private String parse(String html) {
        return Jsoup.parse(html).text();
    }

    private boolean stillContainsTags(String text) {
        return TAG_REGEXP_PATTERN.matcher(text).find();
    }

    private void writeResultToHdfs(Path dst, String result) throws IOException {
        FSDataOutputStream output = null;
        try {
            output = fs.create(dst);
            output.writeBytes(result);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (output != null) {
                output.close();
            }
        }
    }
}
