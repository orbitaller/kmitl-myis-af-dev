FROM apache/airflow:2.2.4

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         libaio1 \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         unzip \
  && mkdir -p /opt/oracle \
  && cd /opt/oracle \
  && curl -LfO https://download.oracle.com/otn_software/linux/instantclient/1914000/instantclient-basic-linux.x64-19.14.0.0.0dbru.zip \
  && unzip ./instantclient-basic-linux.x64-19.14.0.0.0dbru.zip \
  && rm ./instantclient-basic-linux.x64-19.14.0.0.0dbru.zip \
  && apt-get purge -y unzip \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && echo '/opt/oracle/instantclient_19_14' > /etc/ld.so.conf.d/oracle-instantclient.conf \
  && ldconfig

USER airflow

RUN pip install --no-cache-dir 'apache-airflow-providers-oracle'
