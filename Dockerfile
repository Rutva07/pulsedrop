FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml ./
RUN mvn -B -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -B verify

FROM eclipse-temurin:17-jre-jammy AS app
WORKDIR /app
RUN groupadd --system pulsedrop && useradd --system --gid pulsedrop --home-dir /app pulsedrop
COPY --from=build /workspace/target/pulsedrop.jar /app/pulsedrop.jar
USER pulsedrop
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/pulsedrop.jar"]

FROM flink:1.20.1-scala_2.12-java17 AS flink
COPY --from=build /workspace/target/pulsedrop.jar /opt/flink/usrlib/pulsedrop.jar
