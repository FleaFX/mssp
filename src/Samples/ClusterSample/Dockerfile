FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore NuGet packages in a separate layer so it is cached across code changes.
COPY ClusterSample.csproj .
RUN dotnet restore

# Build and publish.
COPY . .
RUN dotnet publish -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "ClusterSample.dll"]
