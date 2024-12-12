# Etapa de construcción
FROM golang:1.22-alpine AS builder

# Cambiar al directorio donde está el archivo go.mod
WORKDIR /proyecto/practica4/CodigoEsqueleto/raft

# Copiar el archivo go.mod y go.sum al contenedor
COPY ../practica4/CodigoEsqueleto/raft/go.* ./

# Descargar las dependencias del proyecto
RUN go mod tidy

# Copiar el resto del código fuente
COPY ../practica4/CodigoEsqueleto/raft/ ./

# Cambiar al directorio donde está el código fuente a compilar
WORKDIR /proyecto/practica4/CodigoEsqueleto/raft/cmd/srvraft

# Compilar el código Go asegurándose de que puede encontrar el archivo go.mod
RUN go build -o srvraft .

# Etapa final: imagen ligera para ejecutar el binario
FROM alpine:latest

# Copiar el binario compilado desde la etapa de construcción
COPY --from=builder /proyecto/practica4/CodigoEsqueleto/raft/cmd/srvraft/srvraft /proyecto/practica4/CodigoEsqueleto/raft/cmd/srvraft/srvraft

COPY ../docker-debug/servidor.sh /proyecto/docker/servidor.sh

# Exponer el puerto 5050
EXPOSE 5050

RUN apk add bash
ENTRYPOINT ["/bin/bash","/proyecto/docker/servidor.sh"]