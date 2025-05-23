# Dockerfile для Maven Java приложения
FROM maven:3.9.6-eclipse-temurin-17 AS maven_build
WORKDIR /workspace
COPY . /workspace
RUN mvn package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /workspace
COPY --from=maven_build /workspace/target/app.jar ./app.jar
CMD ["java", "-jar", "./app.jar"]
