package home.nkavtur.utils;

import java.util.List;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Represents model of user.profile.tags.us.txt file.
 * 
 * @author nkavtur
 */
public class UserProfileTagsEntry {
    private String id;
    private List<String> value;
    private String status;
    private String pricingType;
    private String matchType;
    private String url;
    private String html;

    public String getId() {
        return id;
    }

    public UserProfileTagsEntry withId(String id) {
        this.id = id;
        return this;
    }

    public List<String> getValue() {
        return value;
    }

    public UserProfileTagsEntry withValue(List<String> value) {
        this.value = value;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public UserProfileTagsEntry withStatus(String Status) {
        this.status = Status;
        return this;
    }

    public String getPricingType() {
        return pricingType;
    }

    public UserProfileTagsEntry withPricingType(String pricingType) {
        this.pricingType = pricingType;
        return this;
    }

    public String getMatchType() {
        return matchType;
    }

    public UserProfileTagsEntry withMatchType(String MatchType) {
        this.matchType = MatchType;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public UserProfileTagsEntry withUrl(String url) {
        this.url = url;
        return this;
    }

    public String getHtml() {
        return html;
    }

    public UserProfileTagsEntry withHtml(String html) {
        this.html = html;
        return this;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }
}
