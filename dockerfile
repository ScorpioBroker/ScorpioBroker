FROM maven:3-jdk-11
COPY ./ /usr/src/mymaven
WORKDIR /usr/src/mymaven
RUN mvn clean package -DskipTests -DskipDefault


FROM openjdk:11-jre

WORKDIR /usr/src/scorpio


ARG PROJECT_VERSION=0.9.2-SNAPSHOT

ARG BUILD_DIR_SCS=SpringCloudModules/config-server
ARG BUILD_DIR_SES=SpringCloudModules/eureka
ARG BUILD_DIR_SGW=SpringCloudModules/gateway
ARG BUILD_DIR_SCR=AllInOneRunner

ARG JAR_FILE_BUILD_SCS=config-server-${PROJECT_VERSION}.jar
ARG JAR_FILE_BUILD_SES=eureka-server-${PROJECT_VERSION}.jar
ARG JAR_FILE_BUILD_SGW=gateway-${PROJECT_VERSION}.jar
ARG JAR_FILE_BUILD_SCR=AllInOneRunner-${PROJECT_VERSION}.jar


ARG JAR_FILE_RUN_SCS=config-server.jar
ARG JAR_FILE_RUN_SES=eureka-server.jar
ARG JAR_FILE_RUN_SGW=gateway.jar
ARG JAR_FILE_RUN_SCR=runner.jar

COPY --from=0 /usr/src/mymaven/${BUILD_DIR_SCS}/target/${JAR_FILE_BUILD_SCS} ./scs/${JAR_FILE_RUN_SCS}
COPY --from=0 /usr/src/mymaven/${BUILD_DIR_SES}/target/${JAR_FILE_BUILD_SES} ./ses/${JAR_FILE_RUN_SES}
COPY --from=0 /usr/src/mymaven/${BUILD_DIR_SGW}/target/${JAR_FILE_BUILD_SGW} ./sgw/${JAR_FILE_RUN_SGW}
COPY --from=0 /usr/src/mymaven/${BUILD_DIR_SCR}/target/${JAR_FILE_BUILD_SCR} ./scr/${JAR_FILE_RUN_SCR}



COPY --from=0 /usr/src/mymaven/${BUILD_DIR_SES}/src/main/resources/application-aaio.yml ./ses/config/application.yml
COPY --from=0 /usr/src/mymaven/${BUILD_DIR_SCS}/src/main/resources/application-aaio.yml ./scs/config/application.yml
COPY --from=0 /usr/src/mymaven/${BUILD_DIR_SGW}/src/main/resources/application-aaio.yml ./sgw/config/application.yml
#COPY /usr/src/mymaven/${BUILD_DIR_SCR}/src/main/resources/application-aaio.yml ./scr/config/application.yml

ENV sesdir ses
ENV sesjar ${JAR_FILE_RUN_SES}
ENV scsdir scs
ENV scsjar ${JAR_FILE_RUN_SCS}
ENV sgwdir sgw
ENV sgwjar ${JAR_FILE_RUN_SGW}
ENV scrdir scr
ENV scrjar ${JAR_FILE_RUN_SCR}

COPY run.sh ./

CMD bash ./run.sh