FROM node:22-alpine

WORKDIR /app

COPY package.json tsconfig.json /app/
RUN npm install

COPY src /app/src
RUN npm run build

CMD ["node", "dist/mid-agent/index.js"]
