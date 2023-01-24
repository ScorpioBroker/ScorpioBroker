# Copyright 2022 Engineering Ingegneria Informatica S.p.A.
#
# This file is part of the IoT Agent for the OPCUA protocol (iotagent-opcua) component
#
# iotagent-opcua is free software: you can redistribute it and/or
# modify it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the License,
# or (at your option) any later version.
#
# iotagent-opcua is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public
# License along with iotagent-opcua.
# If not, see http://www.gnu.org/licenses/.
#
# For those usages not covered by the GNU Affero General Public License
# please contact with::[manfredi.pistone@eng.it, gabriele.deluca@eng.it, walterdomenico.vergara@eng.it, mattiagiuseppe.marzano@eng.it]

ARG NODE_VERSION=16
ARG GITHUB_ACCOUNT=Engineering-Research-and-Development
ARG GITHUB_REPOSITORY=iotagent-opcua
ARG DOWNLOAD=latest
ARG SOURCE_BRANCH=master

########################################################################################
#
# This build stage retrieves the source code from GitHub. The default download is the
# latest tip of the master of the named repository on GitHub.
#
# To obtain the latest stable release run this Docker file with the parameters:
# --no-cache --build-arg DOWNLOAD=stable
#
# To obtain any specific version of a release run this Docker file with the parameters:
# --no-cache --build-arg DOWNLOAD=1.7.0
#
# For development purposes, to create a development image including a running Distro,
# run this Docker file with the parameter:
#
# --target=builder
#
########################################################################################
FROM node:${NODE_VERSION} AS builder
ARG GITHUB_ACCOUNT
ARG GITHUB_REPOSITORY
ARG DOWNLOAD
ARG SOURCE_BRANCH

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# As an Alternative for local development, just copy this Dockerfile into file the root of
# the repository and replace the whole RUN statement below by the following COPY statement
# in your local source using :
#
# COPY . /opt/iotagent-opcua/
#

# hadolint ignore=DL3008,DL3005
RUN \
	# Install security updates
	apt-get update -y && \
	apt-get upgrade -y && \
	# Ensure that unzip is installed prior to downloading
	apt-get install -y --no-install-recommends unzip && \
	if [ "${DOWNLOAD}" = "latest" ] ; \
	then \
		RELEASE="${SOURCE_BRANCH}"; \
		echo "INFO: Building Latest Development from ${SOURCE_BRANCH} branch."; \
	elif [ "${DOWNLOAD}" = "stable" ]; \
	then \
		RELEASE=$(curl -s https://api.github.com/repos/"${GITHUB_ACCOUNT}"/"${GITHUB_REPOSITORY}"/releases/latest | grep 'tag_name' | cut -d\" -f4); \
		echo "INFO: Building Latest Stable Release: ${RELEASE}"; \
	else \
	 	RELEASE="${DOWNLOAD}"; \
	 	echo "INFO: Building Release: ${RELEASE}"; \
	fi && \
	RELEASE_CONCAT=$(echo "${RELEASE}" | tr / -); \
	curl -s -L https://github.com/"${GITHUB_ACCOUNT}"/"${GITHUB_REPOSITORY}"/archive/"${RELEASE}".zip > source.zip && \
	unzip source.zip -x "*/test/**" "*/rpm/**" "*/ghpages/**" "*/docs/**" "*/.*" && \
	rm source.zip && \
	mv "${GITHUB_REPOSITORY}-${RELEASE_CONCAT}" /opt/iotagent-opcua && \
	# Remove unzip and clean apt cache
	apt-get clean && \
	apt-get remove -y unzip && \
	apt-get -y autoremove && \
	rm -rf /var/lib/apt/lists/*

WORKDIR /opt/iotagent-opcua

# hadolint ignore=DL3008
RUN \
	# Ensure that Git is installed prior to running npm install
	echo "INFO: npm install --production..." && \
	npm install --only=prod --no-package-lock --no-optional && \
	# Remove Git and clean apt cache
	apt-get clean && \
	apt-get -y autoremove

########################################################################################
#
# This build stage installs PM2 if required.
#
# To create an image using PM2 run this Docker file with the parameter:
#
# --target=pm2-install
#
########################################################################################
FROM node:${NODE_VERSION}-slim AS pm2
ARG GITHUB_ACCOUNT
ARG GITHUB_REPOSITORY
ARG NODE_VERSION

LABEL "maintainer"="FIWARE IoTAgent Team. Engineering Ingegneria Informatica S.p.A."
LABEL "org.opencontainers.image.authors"="walterdomenico.vergara@eng.it,manfredigiuseppe.pistone@eng.it"
LABEL "org.opencontainers.image.documentation"="https://iotagent-opcua.readthedocs.io/en/latest/"
LABEL "org.opencontainers.image.vendor"="Engineering Ingegneria Informatica"
LABEL "org.opencontainers.image.licenses"="AGPL-3.0-only"
LABEL "org.opencontainers.image.title"="IoT Agent for the OPCUA protocol"
LABEL "org.opencontainers.image.description"="An Internet of Things Agent intended to enable capturing data from OPC UA devices on the shopfloor and provide them to the upper levels of a FIWARE-based system."
LABEL "org.opencontainers.image.source"="https://github.com/${GITHUB_ACCOUNT}/${GITHUB_REPOSITORY}"
LABEL "org.nodejs.version"="${NODE_VERSION}"

COPY --from=builder /opt/iotagent-opcua /opt/iotagent-opcua
RUN npm install pm2@4.4.0 -g --no-package-lock --no-optional

USER node
ENV NODE_ENV=production
# Expose 4041 for NORTH PORT
EXPOSE ${IOTA_NORTH_PORT:-4041}
CMD ["pm2-runtime", "/opt/iotagent-opcua/bin/iotagent-opcua", "-- ", "conf/config.js"]

########################################################################################
#
# This build stage creates an anonymous user to be used with the distroless build
# as defined below.
#
########################################################################################
FROM node:${NODE_VERSION} AS anon-user
RUN sed -i -r "/^(root|nobody)/!d" /etc/passwd /etc/shadow /etc/group \
    && sed -i -r 's#^(.*):[^:]*$#\1:/sbin/nologin#' /etc/passwd

########################################################################################
#
# This build stage creates a distroless image for production.
#
# IMPORTANT: For production environments use Docker Secrets to protect values of the
# sensitive ENV variables defined below, by adding _FILE to the name of the relevant
# variable.
#
# - IOTA_AUTH_USER, IOTA_AUTH_PASSWORD - when using Keystone Security
# - IOTA_AUTH_CLIENT_ID, IOTA_AUTH_CLIENT_SECRET - when using OAuth2 Security
#
########################################################################################
FROM gcr.io/distroless/nodejs:${NODE_VERSION} AS distroless
ARG GITHUB_ACCOUNT
ARG GITHUB_REPOSITORY
ARG NODE_VERSION

LABEL "maintainer"="FIWARE IoTAgent Team. Engineering Ingegneria Informatica S.p.A."
LABEL "org.opencontainers.image.authors"="walterdomenico.vergara@eng.it,manfredigiuseppe.pistone@eng.it"
LABEL "org.opencontainers.image.documentation"="https://iotagent-opcua.readthedocs.io/en/latest/"
LABEL "org.opencontainers.image.vendor"="Engineering Ingegneria Informatica"
LABEL "org.opencontainers.image.licenses"="AGPL-3.0-only"
LABEL "org.opencontainers.image.title"="IoT Agent for the OPCUA protocol"
LABEL "org.opencontainers.image.description"="An Internet of Things Agent intended to enable capturing data from OPC UA devices on the shopfloor and provide them to the upper levels of a FIWARE-based system."
LABEL "org.opencontainers.image.source"="https://github.com/${GITHUB_ACCOUNT}/${GITHUB_REPOSITORY}"
LABEL "org.nodejs.version"="${NODE_VERSION}"

COPY --from=builder /opt/iotagent-opcua /opt/iotagent-opcua
COPY --from=anon-user /etc/passwd /etc/shadow /etc/group /etc/
WORKDIR /opt/iotagent-opcua

USER nobody
ENV NODE_ENV=production
# Expose 4041 for NORTH PORT
EXPOSE ${IOTA_NORTH_PORT:-4041}
CMD ["./bin/iotagent-opcua", "-- ", "conf/config.js"]
HEALTHCHECK  --interval=30s --timeout=3s --start-period=10s \
  CMD ["/nodejs/bin/node", "./bin/healthcheck"]


########################################################################################
#
# This build stage creates a node-slim image for production.
#
# IMPORTANT: For production environments use Docker Secrets to protect values of the
# sensitive ENV variables defined below, by adding _FILE to the name of the relevant
# variable.
#
# - IOTA_AUTH_USER, IOTA_AUTH_PASSWORD - when using Keystone Security
# - IOTA_AUTH_CLIENT_ID, IOTA_AUTH_CLIENT_SECRET - when using OAuth2 Security
#
########################################################################################
FROM node:${NODE_VERSION}-slim AS slim
ARG GITHUB_ACCOUNT
ARG GITHUB_REPOSITORY
ARG NODE_VERSION

LABEL "maintainer"="FIWARE IoTAgent Team. Engineering Ingegneria Informatica S.p.A."
LABEL "org.opencontainers.image.authors"="walterdomenico.vergara@eng.it,manfredigiuseppe.pistone@eng.it"
LABEL "org.opencontainers.image.documentation"="https://iotagent-opcua.readthedocs.io/en/latest/"
LABEL "org.opencontainers.image.vendor"="Engineering Ingegneria Informatica"
LABEL "org.opencontainers.image.licenses"="AGPL-3.0-only"
LABEL "org.opencontainers.image.title"="IoT Agent for the OPCUA protocol"
LABEL "org.opencontainers.image.description"="An Internet of Things Agent intended to enable capturing data from OPC UA devices on the shopfloor and provide them to the upper levels of a FIWARE-based system."
LABEL "org.opencontainers.image.source"="https://github.com/${GITHUB_ACCOUNT}/${GITHUB_REPOSITORY}"
LABEL "org.nodejs.version"="${NODE_VERSION}"

COPY --from=builder /opt/iotagent-opcua /opt/iotagent-opcua
WORKDIR /opt/iotagent-opcua

# hadolint ignore=DL3008
RUN \
	# Install security updates
	apt-get update -y && \
	#apt-get upgrade -y && \
	# Ensure that Openssl is installed prior to running the agent
	apt-get install -y --no-install-recommends openssl && \
	# clean apt cache
	apt-get clean && \
	apt-get -y autoremove && \
    rm -rf /var/lib/apt/lists/*

RUN chown -R node:node /opt/iotagent-opcua

USER node
ENV NODE_ENV=production
# Expose 4041 for NORTH PORT
EXPOSE ${IOTA_NORTH_PORT:-4041}
CMD ["node", "/opt/iotagent-opcua/bin/iotagent-opcua", "-- ", "conf/config.js"]
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
   CMD ["npm", "run", "healthcheck"]

