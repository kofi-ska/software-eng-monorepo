FROM gradle:8.9-jdk21 AS builder

WORKDIR /app

COPY . .

RUN gradle clean bootJar -x test --no-daemon

FROM eclipse-temurin:21-jdk

WORKDIR /app

COPY --from=builder /app/build/libs/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
