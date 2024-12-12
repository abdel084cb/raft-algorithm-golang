#!/bin/bash

# Obtener la IP de la interfaz eth0 (ajusta esto según tu interfaz de red)
IP=$(ip addr show eth0 | grep 'inet ' | awk '{print $2}' | cut -d'/' -f1)
echo "$IP"

# Verificar si se obtuvo una IP, si no, salir con error
if [[ -z "$IP" ]]; then
  echo "Error: No se pudo obtener la IP de la interfaz eth0." >&2
  exit 1
fi

# Cambiar a la ruta del programa
if ! cd /proyecto/practica4/CodigoEsqueleto/raft/cmd/srvraft; then
  echo "Error: No se pudo cambiar al directorio /proyecto/practica3/CodigoEsqueleto/raft/cmd/srvraft." >&2
  exit 1
fi

# Lista de IP:PUERTO de todos los nodos
ips="172.20.100.2:8000 172.20.100.3:8001 172.20.100.4:8002 172.20.100.5:8003 172.20.100.6:8004"

# Vector para almacenar las IPs del resto
ipsDelResto=""

# Obtener el índice de la máquina actual dentro de la lista de nodos
index=0
for i in $ips; do
  ip_actual=$(echo $i | cut -d':' -f1)  # Extraer solo la IP
    ipsDelResto="$ipsDelResto $i"
    if [[ "$ip_actual" == "$IP" ]]; then
      me=$index  # Guardar el índice de la máquina actual
    fi
  index=$((index + 1))
done

# Ejecutar el programa pasando el índice y las IPs del resto
./srvraft "$me" $ipsDelResto