FROM mcr.microsoft.com/playwright:v1.57.0-jammy

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev

COPY src ./src
ENV NODE_ENV=production

CMD ["node", "src/run.js"]
