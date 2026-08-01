# HuddleCluster — production image
#
# One image, two roles selected at runtime via CMD/args:
#   docker run huddlecluster master start --port 7070 ...
#   docker run huddlecluster agent  start --id web-1 --master http://master:7070 --port 8080
#
# Multi-stage: build wheel in a full image, install into a slim
# runtime image so the final image doesn't carry build toolchains.

# Build stage 
FROM python:3.12-slim AS build

WORKDIR /src
COPY pyproject.toml README.md ./
COPY huddle_cluster_pkg ./huddle_cluster_pkg
COPY huddle_cluster.py ./

RUN pip install --no-cache-dir build && python -m build --wheel

# Runtime stage 
FROM python:3.12-slim AS runtime

# Non-root user — don't run the control plane as root in production.
RUN groupadd --gid 1000 huddle && \
    useradd --uid 1000 --gid huddle --shell /bin/false --create-home huddle

COPY --from=build /src/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -rf /tmp/*.whl

# Writable location for state_file / TLS certs mounted at runtime.
RUN mkdir -p /var/lib/huddle && chown huddle:huddle /var/lib/huddle

USER huddle
WORKDIR /home/huddle

# No EXPOSE default — master and agent use different ports and it's
# caller-configured via --port; document the common master default.
EXPOSE 7070

ENTRYPOINT ["huddle-cluster"]
CMD ["--help"]