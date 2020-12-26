FROM postgres:12-alpine

ENV NODE_VERSION=14.15.3
RUN apk add curl
RUN apk add nodejs npm

RUN node --version
RUN npm --version

WORKDIR /app
COPY package*.json ./
RUN npm install

COPY . .
CMD [ "npm", "start" ]

