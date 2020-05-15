# Build in a Maven image:
FROM maven:3-jdk-8 AS build-env

# Allow e.g. proxy setting via build-arg if needed:
ARG MAVEN_OPTS

WORKDIR /webarchive-discovery
COPY . .
#RUN mvn package -q -DskipTests
RUN mvn package -DskipTests

# Install the JARs in a clean image:
FROM openjdk:8-jre

MAINTAINER Andrew.Jackson@bl.uk

COPY --from=build-env /webarchive-discovery/warc-indexer/target/warc-indexer-*-jar-with-dependencies.jar /jars/warc-indexer.jar
COPY --from=build-env /webarchive-discovery/warc-hadoop-recordreaders/target/warc-hadoop-recordreaders-*-job.jar /jars/warc-hadoop-recordreaders-job.jar
COPY --from=build-env /webarchive-discovery/warc-hadoop-indexer/target/warc-hadoop-indexer-*-job.jar /jars/warc-hadoop-indexer-job.jar
COPY --from=build-env /webarchive-discovery/configs /jars/configs

