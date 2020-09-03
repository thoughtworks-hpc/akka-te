FROM openjdk:8-jdk-alpine

ARG JAR_FILE=target/app-1.0-allinone.jar

WORKDIR /usr/local/akka-te

COPY ${JAR_FILE} app.jar
COPY config/local_gateway.conf ./config/local_gateway.conf

ENTRYPOINT ["java", "-Dconfig.file=./config/local_gateway.conf", "-jar", "app.jar"]