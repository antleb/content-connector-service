# content-connector-service #

Project that facilitates the service where every content connector provider
 can integrate the ContentConnector API.



## deploying

In order to deploy the service there are some environmental variables that
should be already available to the system that will handle the deployment.

Content service is actually deployed with a docker container, so the environmental
variables can be added while creating the container or the docker service
(as a service of a docker swarm cluster).

The variables are located in the (not publicly shared) file named content-service.env.
In the (also not publicly shared) file tokens-override.properties the variables
concern the local deployment for testing.

## testing

Several JUnit testing methods have been implemented and split in a couple of files.

ContentServiceBrowseTest contains methods to test only browsing and searching within contents.
ContentServiceBuildingTest contains methods to test preparation and building of corpus.

Because the latter needs an authenticated user to actually execute the building process,
it takes advantage of the configuration files that are in the testing resources directory.
In addition to that, (depending of the IDE used) it may be needed to declare the environmental variables
per method. i.e. for IntelliJ IDEA, in the configuration of each test to run there is the
environmental variables field where the user can set environmental variables as key-value pairs.




