FROM amazonlinux:2

# Add the NodeSource repository for Node.js
RUN curl -sL https://rpm.nodesource.com/setup_14.x | bash -

# Install Node.js, npm, and git
RUN yum update -y && \
    yum install -y nodejs npm git

# Set the working directory
WORKDIR /app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the application code
COPY . .

# Expose the application port
EXPOSE 8110

# Set the entry point command
CMD ["node", "distribution.js"]