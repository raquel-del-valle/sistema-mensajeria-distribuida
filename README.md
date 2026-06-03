# Sistema de Mensajería Distribuida Multi-Tecnología

Este proyecto es el resultado final de la asignatura de Sistemas Distribuidos (3º curso de Ingeniería Informática en la UC3M). 

Consiste en una aplicación de mensajería distribuida y concurrente que integra tres tecnologías de comunicación diferentes: **Sockets TCP**, **REST/HTTP** y **ONC-RPC**.

## Arquitectura del Sistema
El sistema se compone de cuatro piezas fundamentales diseñadas para desplegarse en contenedores con direcciones IP distintas:

*   **Servidor de Mensajería (`server.c`):** Servidor central concurrente multihilo desarrollado en C. Gestiona el ciclo de vida de los usuarios, conexiones efímeras y el almacenamiento dinámico de mensajes pendientes mediante listas enlazadas protegidas por exclusión mutua (mutex).
*   **Cliente de Usuario (`client.py`):** Interfaz desarrollada en Python. Implementa un modelo de doble hilo: uno para la interacción del usuario y un hilo en segundo plano en modo *daemon* escuchando en un puerto local para recibir mensajes de forma asíncrona.
*   **Servicio Web REST (`web_service.py`):** Microservicio local desarrollado con Flask (Python) que recibe los mensajes vía HTTP POST y los devuelve normalizados (eliminando espacios repetidos) antes de su envío al servidor principal.
*   **Servicio de Log RPC (`logRPC_server.c`):** Servidor ONC-RPC (C) que recibe y registra en tiempo real las operaciones realizadas en el servidor principal, comunicándose mediante una interfaz IDL (`.x`).

## Características Destacadas
*   **Transferencia *Peer-to-Peer* (P2P):** Los ficheros adjuntos se transfieren directamente entre clientes mediante Sockets TCP, sin sobrecargar el servidor central.
*   **Tolerancia a fallos:** El servidor gestiona desconexiones abruptas, y el cliente aplica normalización local si el microservicio REST no está disponible.
*   **Compatibilidad binaria:** Uso de la librería `struct` en Python para empaquetar y desempaquetar bytes de estado y comunicarse limpiamente con el servidor en C.

## Despliegue y Ejecución
Para compilar los módulos en C, utiliza el `Makefile` incluido:
```bash 
make all
```
Orden de arranque (ejemplo en contenedores o terminales distintas):
**Nodo 1 (Servidor RPC):** 
```bash
   ./logRPC_server
```
**Nodo 2 (Servidor de Mensajería):**
```bash
   export LOG_RPC_IP=<IP_NODO_1>
   ./server -p 8888
```
**Nodos Cliente: En cada máquina cliente, lanza primero el microservicio web y luego la interfaz:**
```bash
   python3 web_service.py
   python3 client.py -s <IP_NODO_2> -p 8888
```
