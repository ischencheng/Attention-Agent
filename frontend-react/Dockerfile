FROM node:20-alpine

WORKDIR /app

COPY package.json /app/package.json
RUN npm install

COPY index.html /app/index.html
COPY vite.config.js /app/vite.config.js
COPY src /app/src

CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]
