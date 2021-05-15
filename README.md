# Problema Productor-Consumidor | Proyecto Final
Es un problema de sincronización de multiprocesos. Hay dos tipos de procesos, productores y consumidores que consumen un buffer de tamaño limitado compartido.

En este proyecto los productores es dado por el archivo `compradores.csv`, los consumidores y el tamaño del buffer son definidos por el usuario a traves de un argumento en la linea de comando.

El programa tiene dos formas de ejecución; con alternancia y sin alternacia. Con alternancia los productores van a trabajar hasta llenar el buffer y después los consumidores van a trabajar hasta vaciarlo, sin alternacia los productores y consumidores van a correr en simultaneo.

## Instrucciones para correr
### Como notebook
1. Correr el siguiente comando:
```bash
docker-compose up
```
2. Acceder a `localhost:8888`
3. Abrir el archivo `Untitled.pynb`

### Como CLI
1. Correr el siguiente comando:
```bash
docker-compose up
```
2. Correr lo siguientes comandos
```bash
pip i -r requirements.txt 

python Prodconsumidor.py buffsize=5 productores=5 consumidor=./notebooks/data/compradores.csv alternancia=0 debug=0
```
