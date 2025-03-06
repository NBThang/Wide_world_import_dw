FROM quay.io/astronomer/astro-runtime:12.7.1

USER root
RUN pip install dbt-bigquery
USER astro