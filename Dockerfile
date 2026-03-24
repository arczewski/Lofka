# Stage 1: Build with Native AOT
FROM mcr.microsoft.com/dotnet/sdk:10.0-preview AS build

# Install Native AOT prerequisites
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy csproj for layer caching
COPY src/Lofka.Server/Lofka.Server.csproj src/Lofka.Server/
RUN dotnet restore src/Lofka.Server/Lofka.Server.csproj -r linux-x64

# Copy source and publish
COPY src/ src/
RUN dotnet publish src/Lofka.Server/Lofka.Server.csproj \
    -c Release \
    -r linux-x64 \
    -o /app \
    --no-restore

# Stage 2: Minimal runtime image
# Native AOT needs no .NET runtime — just libc and libssl
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-preview-noble-chiseled AS runtime

WORKDIR /app
COPY --from=build /app/Lofka.Server .

EXPOSE 9092

ENTRYPOINT ["/app/Lofka.Server"]
