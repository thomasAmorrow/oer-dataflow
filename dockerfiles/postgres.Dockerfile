# Start from official PostGIS image (based on PostgreSQL 16 + PostGIS 3.5.2)
FROM postgres:16-bullseye

LABEL maintainer="PostGIS Project - https://postgis.net" \
      org.opencontainers.image.description="PostGIS 3.5.2+dfsg-1.pgdg110+1 spatial database extension with PostgreSQL 16 bullseye" \
      org.opencontainers.image.source="https://github.com/postgis/docker-postgis"

ENV POSTGIS_MAJOR 3
ENV POSTGIS_VERSION 3.5.2+dfsg-1.pgdg110+1

# Install PostGIS
RUN apt-get update \
    && apt-cache showpkg postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
    && apt-get install -y --no-install-recommends \
            ca-certificates \
            postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR=$POSTGIS_VERSION \
            postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       python3 python3-pip \
       make build-essential postgresql-server-dev-16 git wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Use PGDG repo for postgresql-16-h3 instead of sid
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget gnupg2 \
    && wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && echo "deb http://apt.postgresql.org/pub/repos/apt bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-16-h3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Optional: Install CMake if needed
# RUN wget https://github.com/Kitware/CMake/releases/download/v3.24.2/cmake-3.24.2-linux-x86_64.sh -O /tmp/cmake.sh && \
#     chmod +x /tmp/cmake.sh && \
#     /tmp/cmake.sh --skip-license --prefix=/usr/local && \
#     rm /tmp/cmake.sh

# Optional: Install H3 extension using pgxn
# RUN pgxn install h3

# Clean up apt cache to reduce image size
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Expose port for PostgreSQL
EXPOSE 5432

# Set up PostGIS initialization scripts
RUN mkdir -p /docker-entrypoint-initdb.d
RUN wget https://raw.githubusercontent.com/postgis/docker-postgis/refs/heads/master/16-3.5/initdb-postgis.sh -O /docker-entrypoint-initdb.d/10_postgis.sh
RUN wget https://raw.githubusercontent.com/postgis/docker-postgis/refs/heads/master/16-3.5/update-postgis.sh -O /usr/local/bin/update-postgis.sh
