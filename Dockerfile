FROM tomcat:9.0-alpine
MAINTAINER "stevengatsios@gmail.com"

RUN ["rm", "-fr", "/usr/local/tomcat/webapps/"]
COPY ./target/content-connector-service.war /usr/local/tomcat/webapps/content-connector-service.war
#ENV override-name dev
CMD ["catalina.sh", "run"]