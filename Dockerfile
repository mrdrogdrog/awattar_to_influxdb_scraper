FROM ghcr.io/pnpm/pnpm:11.12.0 AS builder
USER node
WORKDIR /usr/src/app
COPY --chown=node . .
RUN pnpm install --frozen-lockfile

FROM node:26.5.0-alpine AS runner
COPY --chown=node . .
COPY --from=builder --chown=node /usr/src/app/node_modules node_modules
CMD ["node", "index.mjs"]
