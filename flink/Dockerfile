FROM flink:1.19.1-scala_2.12-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev build-essential openjdk-17-jdk-headless \
        ca-certificates curl \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && ln -s "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /opt/java/build-jdk \
    && rm -rf /var/lib/apt/lists/*

# The upstream image supplies a JRE. PyFlink's pemja dependency compiles JNI
# extensions on ARM64 and needs both the JDK headers and the Python C headers.
# Keep a stable, architecture-independent path for build and runtime discovery.
ENV JAVA_HOME=/opt/java/build-jdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Kafka connector jar required by PyFlink KafkaSource/KafkaSink.
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
RUN curl -fSL -o /opt/flink/lib/zstd-jni-1.5.5-11.jar \
    https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-11/zstd-jni-1.5.5-11.jar

COPY bluesky_flink_job.py /opt/flink/usrlib/bluesky_flink_job.py

USER flink
