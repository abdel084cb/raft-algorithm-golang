#!/bin/bash

# Inicia el cliente con los parámetros necesarios
# Modifica los parámetros según tu configuración
/proyecto/practica3/CodigoEsqueleto/raft/cmd/clientraft/clientraft \
  --servidores="servidor:5050" \
  --operaciones="escribir,leer"
