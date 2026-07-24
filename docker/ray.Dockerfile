FROM rayproject/ray:2.49.2-py311
USER root
WORKDIR /opt/project
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[distributed]"
RUN chown -R ray:users /opt/project
USER ray
