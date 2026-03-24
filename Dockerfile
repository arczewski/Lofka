# Stage 1: Build with Native AOT
# The -aot variant includes clang and zlib — no apt-get needed
FROM mcr.microsoft.com/dotnet/sdk:10.0-noble-aot AS build

WORKDIR /src

# Copy csproj for layer caching
COPY src/Lofka.Server/Lofka.Server.csproj src/Lofka.Server/
RUN dotnet restore src/Lofka.Server/Lofka.Server.csproj -r linux-x64

# Copy source and publish
COPY src/ src/
RUN dotnet publish src/Lofka.Server/Lofka.Server.csproj \
    -c Release \
    -r linux-x64 \
    -o /app

# Stage 2: Minimal runtime image
# Native AOT needs no .NET runtime — just libc and libssl
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-noble-chiseled AS runtime

WORKDIR /app
COPY --from=build /app/Lofka.Server .

EXPOSE 9092

ENTRYPOINT ["/app/Lofka.Server"]
