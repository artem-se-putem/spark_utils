# Makefile для util_hive2ch

.PHONY: help keytab_init project_local-build local_start project_oozie-build project_load_oozie project_airflow-build project_load_airflow

help:
	@echo "Available commands:"
	@echo "  make keytab_init          - Initialize Kerberos keytab"
	@echo "  make project_local-build  - Build for local execution"
	@echo "  make local_start          - Run locally"
	@echo "  make project_oozie-build  - Build for Oozie"
	@echo "  make project_load_oozie   - Load to HDFS for Oozie"
	@echo "  make project_airflow-build - Build for Airflow"
	@echo "  make project_load_airflow  - Load to HDFS for Airflow"

keytab_init:
	source common.sh && keytab_init

project_local-build:
	source common.sh && LocalStartBuild
	source common.sh && CredsBuild
	source common.sh && GenerateDDLBuild
	source common.sh && JobBuild

local_start:
	bash local_start.sh

project_oozie-build:
	source common.sh && OozieStartBuild
	source common.sh && CredsBuild

project_load_oozie:
	source common.sh && ProjectLoadOozie

project_airflow-build:
	source common.sh && DagBuild
	source common.sh && CredsBuild

project_load_airflow:
	source common.sh && ProjectLoadAirflow
