FROM apache/hive:3.1.3
USER root

# Bullseye chegou ao EOL — redireciona para o repositório de arquivo
RUN sed -i 's|http://deb.debian.org/debian-security|http://archive.debian.org/debian-security|g' /etc/apt/sources.list \
    && sed -i '/bullseye-updates/d' /etc/apt/sources.list \
    && apt-get update -o Acquire::Check-Valid-Until=false \
    && apt-get install -y --no-install-recommends netcat-openbsd wget \
    && rm -rf /var/lib/apt/lists/*

# JARs necessários: driver PostgreSQL + suporte S3A (MinIO)
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
        -O /opt/hive/lib/postgresql-42.7.3.jar \
    && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar \
        -O /opt/hive/lib/hadoop-aws-3.1.0.jar \
    && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar \
        -O /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar

COPY conf/metastore-site.xml /opt/hive/conf/hive-site.xml
USER hive
