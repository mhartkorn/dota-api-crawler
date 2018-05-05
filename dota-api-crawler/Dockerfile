FROM microsoft/dotnet:2.0-runtime-deps AS runtime

WORKDIR /
COPY publish/* /
COPY schema.sql /
VOLUME database
ENTRYPOINT ["./dota-api-crawler"]
