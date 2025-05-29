FROM node:22

# Create app directory
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# Copy app source
COPY . .

# Install app dependencies
RUN npm install

# Start app
CMD ["npm", "run", "start"]

