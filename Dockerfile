COPY . /app

# Compile and package the application
RUN sbt clean compile stage

# Use the OpenJDK image for running the application
FROM openjdk:11-jre-slim

# Copy the binary files from the previous stage
COPY --from=build /app/target/universal/stage /app

# Set the working directory in the container
WORKDIR /app

# Make port 9000 available to the world outside this container
EXPOSE 9002

# Define environment variable
ENV PLAY_HTTP_SECRET=thisisasecretfortheapplicationandwekeepittosecuretheapplicationletuscheckandithastowork

# Run the binary script when the container launches
CMD ["./bin/scala-play", "-Dplay.http.secret.key=$PLAY_HTTP_SECRET"]