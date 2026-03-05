FROM eclipse-temurin:17-jre
WORKDIR /app
COPY target/pageview-data-producer-1.0-SNAPSHOT-exec.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
