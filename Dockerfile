# Stage 1: Build the application
FROM rust:1.88-bullseye AS builder

WORKDIR /app

# Copy the source code into the builder
COPY ./killbot-rust/ .

# Build the application in release mode.
RUN cargo build --release

# Stage 2: Create the final, minimal image
FROM debian:bullseye

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/killbot-rust .

# The config directory will be mounted as a volume by docker-compose.
# No need to copy it here.

# Set the command to run the application
CMD ["./killbot-rust"]
