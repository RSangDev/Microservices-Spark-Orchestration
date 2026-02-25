# ─── Dockerfile ──────────────────────────────────────────────────────────────
# Imagem customizada do Airflow 2.8.1 com suporte a PySpark 3.5.
#
# Baseada na imagem oficial do Airflow. Adiciona:
#   - Java 17 (requisito do Spark)
#   - PySpark e dependências AWS/dados
#   - spark-submit disponível no PATH
#   - Configurações de segurança e não-root
#
# Build:
#   docker build -t airflow-microservices:2.8.1 .
#
# Uso via Compose:
#   Substituir 'image: apache/airflow:2.8.1' por 'build: .' no docker-compose.yml
# ─────────────────────────────────────────────────────────────────────────────

FROM apache/airflow:2.8.1-python3.11

# ── Metadados ─────────────────────────────────────────────────────────────────
LABEL maintainer="platform-engineering"
LABEL version="2.8.1-spark3.5"
LABEL description="Airflow com PySpark para orquestração de microserviços"

# ── Variáveis de build ────────────────────────────────────────────────────────
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
ARG JAVA_VERSION=17

# Diretório de instalação do Spark
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${JAVA_HOME}/bin:${PATH}"

# Configs PySpark para apontar ao cluster externo (override via docker-compose)
ENV PYSPARK_PYTHON=python3.11
ENV PYSPARK_DRIVER_PYTHON=python3.11

# ── Dependências de sistema (root) ────────────────────────────────────────────
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
        # Java — requisito do Spark
        openjdk-${JAVA_VERSION}-jdk-headless \
        # Utilitários de rede e diagnóstico
        curl \
        wget \
        netcat-traditional \
        procps \
        # Compilação de pacotes Python com extensões C
        gcc \
        g++ \
        libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Instalação do Spark ───────────────────────────────────────────────────────
# O tarball deve estar em docker/ antes do build (ver RUNBOOK — Passo 1).
# Evita dependência de rede durante o build.
COPY --chown=root:root docker/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /tmp/spark.tgz
RUN tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# ── JARs adicionais para integração S3 (hadoop-aws + aws-java-sdk) ───────────
# Baixados manualmente e colocados em docker/ antes do build (ver RUNBOOK).
COPY --chown=root:root docker/hadoop-aws-3.3.4.jar            ${SPARK_HOME}/jars/
COPY --chown=root:root docker/aws-java-sdk-bundle-1.12.262.jar ${SPARK_HOME}/jars/

# ── Permissões ────────────────────────────────────────────────────────────────
RUN chown -R airflow:root ${SPARK_HOME} && \
    chmod -R 775 ${SPARK_HOME}

# Diretório para logs do Spark dentro do worker Airflow
RUN mkdir -p /opt/airflow/spark-logs && \
    chown airflow:root /opt/airflow/spark-logs

# ── Volta para usuário airflow (não-root) ─────────────────────────────────────
USER airflow

# ── Dependências Python ───────────────────────────────────────────────────────
# Copia e instala requirements antes do código fonte (aproveita cache de layer)
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# ── Código do projeto ─────────────────────────────────────────────────────────
COPY --chown=airflow:root dags/       /opt/airflow/dags/
COPY --chown=airflow:root plugins/    /opt/airflow/plugins/
COPY --chown=airflow:root spark_jobs/ /opt/airflow/spark_jobs/
COPY --chown=airflow:root tests/      /opt/airflow/tests/
COPY --chown=airflow:root config/     /opt/airflow/config/

# ── Configuração do Spark dentro do Airflow ───────────────────────────────────
# spark-defaults.conf aplicado a todos os jobs submetidos pelos workers
RUN mkdir -p ${SPARK_HOME}/conf
COPY --chown=airflow:root config/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# ── Health check da imagem ────────────────────────────────────────────────────
# Verifica que Java e spark-submit estão acessíveis
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD spark-submit --version > /dev/null 2>&1 && java -version > /dev/null 2>&1 || exit 1

# ── Ponto de entrada padrão (herdado da imagem base do Airflow) ───────────────
# Não sobrescrever — a imagem base gerencia airflow webserver/scheduler/worker