#!/bin/bash

# Inicia el cliente con los parámetros necesarios
/proyecto/practica4/CodigoEsqueleto/raft/cliente/client \
  --servidores="servidor:5050" \
  --operaciones="escribir,leer"
