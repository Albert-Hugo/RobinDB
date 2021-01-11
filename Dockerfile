FROM openjdk:8
COPY distribution /usr/src/app

EXPOSE 8888
EXPOSE 8688
CMD chmod +x /usr/src/app/RobinDB-1.0.1-SNAPSHOT/bin/*
CMD bash /usr/src/app/RobinDB-1.0.1-SNAPSHOT/bin/startup.sh