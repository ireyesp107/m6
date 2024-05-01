FROM amazonlinux:2

RUN sudo yum update -y && \
    sudo yum install -y nodejs npm git

# Set the working directory
WORKDIR /app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install dependencies
RUN sudo npm install

# Copy the application code
COPY . .

# Expose the application port
EXPOSE 8110

# Set the entry point command
CMD ["node", "distribution.js"]