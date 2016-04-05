package home.nkavtur.utils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import com.google.common.base.Joiner;

/**
 * @author nkavtur
 */
public final class CsvConverter {
    private static final String[] HEADERS = { "KeywordId", "KeywordValue", "KeywordStatus", "pricingType",
            "KeywordMatchType", "destinationUrl" };

    @SuppressWarnings("rawtypes")
    public static String toCsv(List<UserProfileTagsEntry> entries) throws IOException {
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        String targetCsv = null;
        try (StringWriter writer = new StringWriter();
            CSVPrinter csvFilePrinter = new CSVPrinter(writer, csvFileFormat)) {
            csvFilePrinter.printRecord(HEADERS);
            for (UserProfileTagsEntry tag : entries) {
                List record = new ArrayList();
                record.add(tag.getId());
                record.add(Joiner.on(";").join(tag.getValue()));
                record.add(tag.getStatus());
                record.add(tag.getPricingType());
                record.add(tag.getMatchType());
                record.add(tag.getUrl());
                csvFilePrinter.printRecord(record);
            }
            targetCsv = writer.toString();
        }

        return targetCsv;
    }
}
