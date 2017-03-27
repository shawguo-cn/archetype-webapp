#https://spring.io/guides/gs/spring-boot-docker/
FROM frolvlad/alpine-oraclejdk8:slim
VOLUME /tmp
ADD archetype-webapp-0.2-SNAPSHOT.jar app.jar
RUN sh -c 'touch /app.jar'
ENV JAVA_OPTS=""
EXPOSE 8090
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /app.jar" ]