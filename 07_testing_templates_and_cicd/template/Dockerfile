# ==============================================================================
# Dataflow Flex Template Dockerfile — Chapter 07
# Base image contains the Dataflow Flex Template launcher harness
# ==============================================================================
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

ARG WORKDIR=/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Install dependencies
COPY requirements.txt ${WORKDIR}/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code
COPY pipelines/ ${WORKDIR}/pipelines/

# Configure the entrypoint script location for the Dataflow Flex Template harness
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipelines/secure_orders_pipeline.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
