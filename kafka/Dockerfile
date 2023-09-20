FROM mcr.microsoft.com/dotnet/runtime:7.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["kafka/kafka.csproj", "kafka/"]
RUN dotnet restore "kafka/kafka.csproj"
COPY . .
WORKDIR "/src/kafka"
RUN dotnet build "kafka.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "kafka.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "kafka.dll"]
