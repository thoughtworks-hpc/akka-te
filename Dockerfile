FROM openjdk:8-jdk-alpine

ARG JAR_FILE=target/app-1.0-allinone.jar

WORKDIR /usr/local/akka-te

COPY ${JAR_FILE} app.jar

COPY docker ./

CMD ["./run.sh"]