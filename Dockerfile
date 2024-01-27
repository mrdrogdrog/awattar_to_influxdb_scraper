FROM node:lts-alpine AS base
USER node
WORKDIR /usr/src/app
COPY --chown=node package.json package.json
COPY --chown=node package-lock.json package-lock.json

FROM base AS builder
RUN npm install --omit=dev

FROM base AS runner
COPY --from=builder --chown=node /usr/src/app/node_modules node_modules
COPY --chown=node index.mjs index.mjs
CMD ["node", "index.mjs"]
