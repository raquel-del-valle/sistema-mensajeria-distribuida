# Variables de compilación
CC = gcc
CFLAGS = -Wall -g -pthread -I/usr/include/tirpc
LDFLAGS = -ltirpc

# Compila todo
all: server logRPC_server

# SERVIDOR DE MENSAJERIA (CLIENTE RPC)
# Compila archivos .c a .o
lines.o: lines.c lines.h
		$(CC) $(CFLAGS) -c lines.c		
server.o: server.c lines.h logRPC.h
		$(CC) $(CFLAGS) -c server.c
logRPC_clnt.o: logRPC_clnt.c logRPC.h
		$(CC) $(CFLAGS) -c logRPC_clnt.c
logRPC_xdr.o: logRPC_xdr.c logRPC.h
		$(CC) -g -pthread -I/usr/include/tirpc -c logRPC_xdr.c

# Genera los ejecutables
server: server.o lines.o logRPC_clnt.o logRPC_xdr.o
		$(CC) $(CFLAGS) -o server server.o lines.o logRPC_clnt.o logRPC_xdr.o $(LDFLAGS)


# SERVIDOR RPC (SERVICIO DE LOG)
# Compila archivos .c a .o
logRPC_server.o: logRPC_server.c logRPC.h
		$(CC) $(CFLAGS) -c logRPC_server.c
logRPC_svc.o: logRPC_svc.c logRPC.h
		$(CC) $(CFLAGS) -c logRPC_svc.c

# Genera los ejecutables
logRPC_server: logRPC_server.o logRPC_svc.o logRPC_xdr.o
		$(CC) $(CFLAGS) -o logRPC_server logRPC_server.o logRPC_svc.o logRPC_xdr.o $(LDFLAGS)


#Limpieza para borrar los binarios y archivos temporales generados
clean:
		rm -f *.o *.so server logRPC_server