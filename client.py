import socket
import threading
from enum import Enum
import argparse
import requests
import struct


class client :

    # ******************** TYPES *********************
    # *
    # * @brief Return codes for the protocol methods
    class RC(Enum) :
        OK = 0
        ERROR = 1
        USER_ERROR = 2

    # ****************** ATTRIBUTES ******************
    _server = None
    _port = -1
    _listen_sock = None
    _listen_thread = None
    _username = None
    _connected_users = {}

    # ******************** METHODS *******************
    # *
    # * @param user - User name to register in the system
    # * 
    # * @return OK if successful
    # * @return USER_ERROR if the user is already registered
    # * @return ERROR if another error occurred
    @staticmethod
    def  register(user) :
        try:
            # (1) Crear el socket TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((client._server, client._port))

            # (2) Empaquetar el mensaje
            message = "REGISTER\0" + user + "\0"
            sock.sendall(message.encode())

            # (3) Leer la respuesta 
            res = client.getRes(sock)
            sock.close()

            # (4) Interpretar la respuesta
            if res == 0:
                print("c> REGISTER OK")
                return client.RC.OK
            elif res == 1:
                print("c> USERNAME IN USE")
                return client.RC.USER_ERROR
            else:
                print("c> REGISTER FAIL")
                return client.RC.ERROR
        except Exception:
            # Si el servidor está caído o hay error en la red
            print("c> REGISTER FAIL")
            return client.RC.ERROR


    # *
    # 	 * @param user - User name to unregister from the system
    # 	 * 
    # 	 * @return OK if successful
    # 	 * @return USER_ERROR if the user does not exist
    # 	 * @return ERROR if another error occurred
    @staticmethod
    def  unregister(user) :
        try:
            # (1) Crear el socket TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((client._server, client._port))

            # (2) Empaquetar el mensaje
            message = "UNREGISTER\0" + user + "\0"
            sock.sendall(message.encode())

            # (3) Leer la respuesta 
            res = client.getRes(sock)
            sock.close()

            # (4) Interpretar la respuesta
            if res == 0:
                print("c> UNREGISTER OK")
                return client.RC.OK
            elif res == 1:
                print("c> USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            else:
                print("c> UNREGISTER FAIL")
                return client.RC.ERROR        
        except Exception:
            # Si el servidor está caído o hay error en la red
            print("c> UNREGISTER FAIL")
            return client.RC.ERROR


    # *
    # * @param user - User name to connect to the system
    # * 
    # * @return OK if successful
    # * @return USER_ERROR if the user does not exist or if it is already connected
    # * @return ERROR if another error occurred
    @staticmethod
    def  connect(user) :
        try:
            # (1) Crear el socket de escucha
            client._listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client._listen_sock.bind(('', 0))
            client._listen_sock.listen(5)
            listen_port = client._listen_sock.getsockname()

            # (2) Crear el hilo marcado como Daemon para que muera si se cierra la app
            client._listen_thread = threading.Thread(target=client._listen_messages, daemon=True)
            client._listen_thread.start()

            # (3) Conectar al servidor central TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((client._server, client._port))

            # (4) Empaquetar el mensaje
            message = "CONNECT\0" + user + "\0" + str(listen_port) + "\0"
            sock.sendall(message.encode())

            # (5) Leer la respuesta 
            res = client.getRes(sock)
            sock.close()

            # (6) Interpretar la respuesta
            if res == 0:
                client._username = user
                print("c> CONNECT OK")
                return client.RC.OK
            elif res == 1:
                client._listen_sock.close()
                print("c> CONNECT FAIL, USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            elif res == 2:
                client._listen_sock.close()
                print("c> USER ALREADY CONNECTED")
                return client.RC.USER_ERROR
            else:
                client._listen_sock.close()
                print("c> CONNECT FAIL")
                return client.RC.ERROR
        except Exception:
            # Si el servidor está caído o hay error en la red
            if client._listen_sock:
                try:
                    client._listen_sock.close()
                except:
                    pass
            print("c> CONNECT FAIL")
            return client.RC.ERROR


    # Mini-servidor que se ejecuta en segundo plano
    @staticmethod
    def  _listen_messages():
        # Función auxiliar para refactorizar la lectura de cadenas
        def refact_recv_string(sock):
            full_str = ""
            while True:
                b = sock.recv(1)
                if not b or b == b'\0':
                    break
                full_str += b.decode()
            return full_str
            
        while True:
            try:
                # Se queda bloqueado esperando a que el servidor central le mande un SEND
                conn, addr = client._listen_sock.accept()

                # Leer Opcode
                op = refact_recv_string(conn)
                
                if op == "SEND_MESSAGE":
                    sender = refact_recv_string(conn)
                    msg_id = refact_recv_string(conn)
                    msg_text = refact_recv_string(conn)
                    
                    # Imprime el mensaje
                    print(f"\ns> MESSAGE {msg_id} FROM {sender}\n{msg_text}\nEND\nc> ", end="", flush=True)
                
                elif op == "SEND_MESS_ACK":
                    msg_id = refact_recv_string(conn)
                    print(f"SEND MESSAGE {msg_id} OK\nc> ", end="", flush=True)
                
                elif op == "SEND_MESSAGE_ATTACH":
                    sender = refact_recv_string(conn)
                    msg_id = refact_recv_string(conn)
                    msg_text = refact_recv_string(conn)
                    fname = refact_recv_string(conn)

                    # Imprime el mensaje con el nombre del fichero adjunto
                    print(f"\ns> MESSAGE {msg_id} FROM {sender}\n{msg_text}\nEND\nFILE {fname}\nc> ", end="", flush=True)

                elif op == "SEND_MESS_ATTACH_ACK":
                    msg_id = refact_recv_string(conn)
                    fname = refact_recv_string(conn)
                    print(f"SENDATTACH MESSAGE {msg_id} {fname} OK\nc> ", end="", flush=True)

                elif op == "GET_FILE":
                    requester = refact_recv_string(conn)
                    req_fname = refact_recv_string(conn)

                    # Abre el fichero local y envía su contenido
                    try:
                        f = open(req_fname, "rb")
                        conn.sendall(struct.pack("B", 0))

                        with f:
                            while True:
                                chunk = f.read(4096)
                                if not chunk:
                                    break
                                conn.sendall(chunk)

                    except Exception:
                        # Si fichero no existe o hay error, se envía status 1 (fail)
                        try:
                            conn.sendall(struct.pack("B", 1))
                        except:
                            pass

                conn.close()
            except Exception:
                break


    # Se pone la lógica de users en esta función exceptuando los prints
    # para poder usar la misma lógica en el Get File
    # no imprime nada por pantalla. Devuelve True si éxito, False si no
    @staticmethod
    def  _ref_users() :
        try:
            # Si no está conectado, no puede pedir la lista
            if client._username is None:
                return False
            
            # (1) Crear el socket TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((client._server, client._port))

            # (2) Empaquetar el mensaje
            message = "USERS\0" + client._username + "\0"
            sock.sendall(message.encode())

            # (3) Leer la respuesta 
            res = client.getRes(sock)

            # (4) Interpretar la respuesta
            if res != 0:
                sock.close()
                return False
            
            # Leer el número de usuarios conectados
            n_str = ""
            while True:
                b = sock.recv(1)
                if not b or b == b'\0':
                    break
                n_str += b.decode()
            n_connected = int(n_str)

            # Leer cada entrada usuario::IP::puerto
            client._connected_users = {}
            for _ in range(n_connected):
                entry = ""
                while True:
                    b = sock.recv(1)
                    if not b or b == b'\0':
                        break
                    entry += b.decode()
                
                # El servidor envía: "usuario::IP::puerto"
                parts = entry.split("::")
                if len(parts) == 3:
                    client._connected_users[parts[0]] = (parts[1], int(parts[2]))

            sock.close()
            return True
        
        except Exception:
            return False


    # *
    # * 
    # * @return OK if successful
    # * @return USER_ERROR if the user does not exist or if it is already connected
    # * @return ERROR if another error occurred
    @staticmethod
    def  users() :
        try:
            # Si no está conectado, no puede pedir la lista
            if client._username is None:
                print("c> CONNECTED USERS FAIL, USER IS NOT CONNECTED")
                return client.RC.ERROR
            
            if not client._ref_users():
                print("c> CONNECTED USERS FAIL")
                return client.RC.ERROR

            n_connected = len(client._connected_users)
            print(f"c> CONNECTED USERS ({n_connected} users connected) OK")
            for name in client._connected_users.keys():
                print(f"{name}")
            return client.RC.OK
           
        except Exception:
            print("c> CONNECTED USERS FAIL")
            return client.RC.ERROR
        
        
    # *
    # * @param user - User name to disconnect from the system
    # * 
    # * @return OK if successful
    # * @return USER_ERROR if the user does not exist
    # * @return ERROR if another error occurred
    @staticmethod
    def  disconnect(user) :
        try:
            # (1) Conectar al servidor central TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((client._server, client._port))

            # (2) Empaquetar el mensaje
            message = "DISCONNECT\0" + user + "\0"
            sock.sendall(message.encode())

            # (3) Leer la respuesta 
            res = client.getRes(sock)
            sock.close()

            # Cerrar el socket de escucha en segundo plano
            if client._listen_sock:
                try:
                    client._listen_sock.close()
                except:
                    pass

            # (4) Interpretar la respuesta
            if res == 0:
                client._username = None
                client._connected_users = {}
                print("c> DISCONNECT OK")
                return client.RC.OK
            elif res == 1:
                print("c> DISCONNECT FAIL, USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            elif res == 2:
                print("c> DISCONNECT FAIL, USER NOT CONNECTED")
                return client.RC.USER_ERROR
            else:
                print("c> DISCONNECT FAIL")
                return client.RC.ERROR
        
        except Exception:
            # Si hay error de comunicaciones, también se mata el hilo (por si acaso)
            if client._listen_sock:
                try:
                    client._listen_sock.close()
                except:
                    pass
            print("c> DISCONNECT FAIL")
            return client.RC.ERROR
        
    @staticmethod
    def  _normalized_message(message):
        """Llama al servicio web para normalizar los espacios en el mensaje
        Si el servicio no está disponible, normaliza localmente"""
        try:
            r = requests.post(url="http://127.0.0.1:5000/normalize", 
                              json={"message": message},
                              headers={'Content-type': 'application/json'},
                              timeout=1)
            if r.status_code == 201:
                return r.text
            else:
                # Si el servicio devuelve error, hace la normalización localmente
                return ' '.join(message.split())
        
        except Exception:
            # Si el servicio no está disponible, hace la normalización localmente
                return ' '.join(message.split())

    # *
    # * @param user    - Receiver user name
    # * @param message - Message to be sent
    # * 
    # * @return OK if the server had successfully delivered the message
    # * @return USER_ERROR if the user is not connected (the message is queued for delivery)
    # * @return ERROR the user does not exist or another error occurred
    @staticmethod
    def  send(user,  message) :
        try:
            # Si no está conectado, no puede enviar mensajes
            if client._username is None:
                print("c> SEND FAIL")
                return client.RC.ERROR
            
            # Limpieza de espacios con servicio web
            message = client._normalized_message(message)

            # Comprobar la longitud del mensaje (máx 255 caracteres)
            if len(message) > 255:
                print("c> SEND FAIL")
                return client.RC.ERROR

            # (1) Conectar al servidor central TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((client._server, client._port))

            # (2) Empaquetar el mensaje
            full_message = "SEND\0" + client._username + "\0" + user + "\0"  + message + "\0"
            sock.sendall(full_message.encode())

            # (3) Leer la respuesta
            res = client.getRes(sock)

            # (4) Interpretar la respuesta 
            if res == 0:
                msg_id = ""
                while True:
                    b = sock.recv(1)
                    if not b or b == b'\0':
                        break
                    msg_id += b.decode()
                sock.close()
                print(f"c> SEND OK - MESSAGE {msg_id}")
                return client.RC.OK
            elif res == 1:
                sock.close()
                print("c> SEND FAIL, USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            else:
                sock.close()
                print("c> SEND FAIL")
                return client.RC.ERROR
        
        except Exception:
            print("c> SEND FAIL")
            return client.RC.ERROR

    # *
    # * @param user    - Receiver user name
    # * @param file    - file  to be sent
    # * @param message - Message to be sent
    # * 
    # * @return OK if the server had successfully delivered the message
    # * @return USER_ERROR if the user is not connected (the message is queued for delivery)
    # * @return ERROR the user does not exist or another error occurred
    @staticmethod
    def  sendAttach(user,  file,  message) :
        try:
            # Si no está conectado, no puede enviar mensajes
            if client._username is None:
                print("c> SENDATTACH FAIL")
                return client.RC.ERROR
            
            # Limpieza de espacios con servicio web
            message = client._normalized_message(message)

            # Comprobar la longitud del mensaje (máx 255 caracteres)
            if len(message) > 255:
                print("c> SENDATTACH FAIL")
                return client.RC.ERROR
            
            # Comprobar la longitud del nombre del archivo (máx 255 caracteres)
            if len(file) > 255:
                print("c> SENDATTACH FAIL")
                return client.RC.ERROR

            # (1) Conectar al servidor central TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((client._server, client._port))

            # (2) Empaquetar el mensaje
            full_message = "SENDATTACH\0" + client._username + "\0" + user + "\0"  + message + "\0" + file + "\0"
            sock.sendall(full_message.encode())

            # (3) Leer la respuesta
            res = client.getRes(sock)

            # (4) Interpretar la respuesta 
            if res == 0:
                msg_id = ""
                while True:
                    b = sock.recv(1)
                    if not b or b == b'\0':
                        break
                    msg_id += b.decode()
                sock.close()
                print(f"c> SENDATTACH OK - MESSAGE {msg_id}")
                return client.RC.OK
            elif res == 1:
                sock.close()
                print("c> SENDATTACH FAIL, USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            else:
                sock.close()
                print("c> SENDATTACH FAIL")
                return client.RC.ERROR
        
        except Exception:
            print("c> SENDATTACH FAIL")
            return client.RC.ERROR
        
    # *
    # * @param user    - Receiver user name
    # * @param file    - file  to be sent
    # * @param message - Message to be sent
    # * 
    # * @return OK if the server had successfully delivered the message
    # * @return USER_ERROR if the user is not connected (the message is queued for delivery)
    # * @return ERROR the user does not exist or another error occurred
    @staticmethod
    def  getFile(user,  fileName, localFileName) :
        sock = None     # Inicializar para que le except no falle si nunca se crea
        try:
            # Buscar al usuario
            if user not in client._connected_users:
                # Pide USERS internamente para refrescar la lista de usuarios
                client._ref_users()

            # Si sigue sin estar, se considera desconectado
            if user not in client._connected_users:
                print("c> FILE TRANSFER FAILED, user not connected.")
                return client.RC.ERROR
            
            # Obtener IP y puerto del hilo de escucha del usuario
            ip, port = client._connected_users[user]

            # (1) Conectar al thread de escucha del usuario remoto
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, port))

            # (2) Enviar la petición GET_FILE
            message = "GET_FILE\0" + client._username + "\0" + fileName + "\0"
            sock.sendall(message.encode())

            # (3) Recibir el contenido del fichero y guardarlo en local
            status_data = sock.recv(1)
            if not status_data:
                sock.close()
                # Este print no está en el enunciado pero se pone por claridad
                print("c> FILE TRANSFER FAILED")
                return client.RC.ERROR
            
            status = struct.unpack("B", status_data)[0]

            if status == 0:
                # Solo se crea el fichero si el servidor confirma que existe
                with open(localFileName, "wb") as f:
                    while True:
                        data = sock.recv(4096)
                        if not data:
                            break
                        f.write(data)
            
                sock.close()
                # Este print no está en el enunciado pero se pone por claridad
                print(f"c> GETFILE {fileName} OK")
                return client.RC.OK
            
            else:
                sock.close()
                print("c> FILE TRANSFER FAILED")
                return client.RC.ERROR

        except Exception:
            if sock is not None:
                try:
                    sock.close()
                except:
                    pass            
            # Este print no está en el enunciado pero se pone por claridad
            print("c> FILE TRANSFER FAILED")
            return client.RC.ERROR

    # *
    # **
    # * @brief Command interpreter for the client. It calls the protocol functions.
    @staticmethod
    def shell():

        while (True) :
            try :
                command = input("c> ")
                line = command.split(" ")
                if (len(line) > 0):

                    line[0] = line[0].upper()

                    if (line[0]=="REGISTER") :
                        if (len(line) == 2) :
                            client.register(line[1])
                        else :
                            print("Syntax error. Usage: REGISTER <userName>")

                    elif(line[0]=="UNREGISTER") :
                        if (len(line) == 2) :
                            client.unregister(line[1])
                        else :
                            print("Syntax error. Usage: UNREGISTER <userName>")

                    elif(line[0]=="CONNECT") :
                        if (len(line) == 2) :
                            client.connect(line[1])
                        else :
                            print("Syntax error. Usage: CONNECT <userName>")

                    elif(line[0]=="DISCONNECT") :
                        if (len(line) == 2) :
                            client.disconnect(line[1])
                        else :
                            print("Syntax error. Usage: DISCONNECT <userName>")

                    elif(line[0]=="USERS") :
                        if (len(line) == 1) :
                            client.users()
                        else :
                            print("Syntax error. Usage: CONNECTED_USERS <userName>")

                    elif(line[0]=="SEND") :
                        if (len(line) >= 3) :
                            #  Remove first two words
                            message = ' '.join(line[2:])
                            client.send(line[1], message)
                        else :
                            print("Syntax error. Usage: SEND <userName> <message>")

                    elif(line[0]=="SENDATTACH") :
                        if (len(line) >= 4) :
                            fileName = line[-1]
                            message = ' '.join(line[2:-1])
                            client.sendAttach(line[1], fileName, message)
                        else :
                            print("Syntax error. Usage: SENDATTACH <userName> <message> <filename>")

                    elif(line[0]=="GETFILE") :
                        if (len(line) == 4) :
                            client.getFile(line[1], line[2], line[3])
                        else :
                            print("Syntax error. Usage: GETFILE <userName> <filename> <localFileName>")

                    elif(line[0]=="QUIT") :
                        if (len(line) == 1) :
                            if client._username is not None:
                                client.disconnect(client._username)
                            break
                        else :
                            print("Syntax error. Use: QUIT")
                    else :
                        print("Error: command " + line[0] + " not valid.")
            except Exception as e:
                print("Exception: " + str(e))

    # *
    # * @brief Prints program usage
    @staticmethod
    def usage() :
        print("Usage: python3 client.py -s <server> -p <port>")


    # *
    # * @brief Parses program execution arguments
    @staticmethod
    def  parseArguments(argv) :
        parser = argparse.ArgumentParser()
        parser.add_argument('-s', type=str, required=True, help='Server IP')
        parser.add_argument('-p', type=int, required=True, help='Server Port')
        args = parser.parse_args()

        if (args.s is None):
            parser.error("Usage: python3 client.py -s <server> -p <port>")
            return False

        if ((args.p < 1024) or (args.p > 65535)):
            parser.error("Error: Port must be in the range 1024 <= port <= 65535");
            return False
        
        client._server = args.s
        client._port = args.p

        return True


    # *
    # * @brief reads the binary code from server
    @staticmethod
    def getRes(sock):
        data = sock.recv(1)
        return data[0] if data else None


    # ******************** MAIN *********************
    @staticmethod
    def main(argv) :
        if (not client.parseArguments(argv)) :
            client.usage()
            return

        #  Write code here
        client.shell()
        print("+++ FINISHED +++")
    

if __name__=="__main__":
    client.main([])
