# Crawl-On-Yarn Application #

## Crawls html pages from user.profile.tags.us.txt and calculates top 10 words with highest frequency.  ##
## To setup and execute application follow next steps: ##

1) Create yarn user in hdfs:

```
#!bash

su hdfs -c "hdfs dfs -mkdir /user/yarn"
su hdfs -c "hdfs dfs -chown yarn /user/yarn"
```
2) Go to application home directory and execute:
```
#!bash
mvn clean package
```
3) Copy ${application_home}/target/crawl-on-yarn-0.0.1.jar, ${application_home}/log4j.properties, ${application_home}/user.profile.tags.us.txt to /opt directory:
```
#!bash
cp ${application_home}/target/crawl-on-yarn-0.0.1.jar /opt
cp ${application_home}/log4j.properties /opt
cp ${application_home}/user.profile.tags.us.txt /opt
```
4) Run the application:
```
#!bash
su yarn -c "java -cp $(hadoop classpath):/etc/hadoop/*:/opt/crawl-on-yarn-0.0.1.jar home.nkavtur.client.CrawlAppClient -log_properties /opt/log4j.properties -user_profile_tags /opt/user.profile.tags.us.txt"
```
5) After the application completes, copy resulting file from hdfs to localFS to see the result:
```
#!bash
su yarn -c "hdfs dfs -copyToLocal Crawl-On-Yarn-App/{appId}/res.csv /home/yarn"
```


## Logs are available in here:  ##
* /var/log/hadoop/yarn/CrawlApplicationMaster.stderr
* /var/log/hadoop/yarn/CrawlApplicationMaster.stderr