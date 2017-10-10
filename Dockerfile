FROM tomcat:9.0-alpine
MAINTAINER "stevengatsios@gmail.com"

RUN ["rm", "-fr", "/usr/local/tomcat/webapps/"]
COPY ./target/content-connector-service.war /usr/local/tomcat/webapps/content-connector-service.war
ENV spring.profiles.active services
CMD ["catalina.sh", "run"]