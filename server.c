#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <netdb.h>
#include <signal.h>
#include "lines.h"
#include "logRPC.h"

#define MAX_LINE        256

// Función auxiliar para enviar el log al servicio auxiliar RPC
static void log_to_RPC (const char *user, const char *operation, const char *filename) {
    char *rpc_ip = getenv("LOG_RPC_IP");
    // Si no está definida, no se hace el log
    if (rpc_ip == NULL) {
        return;
    }

    CLIENT *clnt = clnt_create(rpc_ip, LOG, LOG_VERSION, "tcp");
    // Si el RPC falla, continúa el resto con normalidad
    if (clnt == NULL) {
        return;
    }

    log_arg arg;
    memset(&arg, 0, sizeof(arg));
    if (user != NULL) {
        strncpy(arg.username, user, 255);
    } else {
        strncpy(arg.username, "", 255);
    }
    arg.username[255] = '\0';

    if (operation != NULL) {
        strncpy(arg.operation, operation, 31);
    } else {
        strncpy(arg.operation, "", 31);
    }
    arg.operation[31] = '\0';

    if (filename != NULL) {
        strncpy(arg.filename, filename, 255);
    } else {
        strncpy(arg.filename, "", 255);
    }
    arg.filename[255] = '\0';

    int result;
    enum clnt_stat retval = rpc_log_1(arg, &result, clnt);
    if (retval != RPC_SUCCESS) {
        clnt_perror(clnt, "rpc_log call failed");
    }
    clnt_destroy(clnt);
}

// Estructura para los mensajes almacenados (cola de mensajes pendientes)
struct MessageNode {
    char sender[256];
    unsigned int id;
    char message[256];
    char filename[256];
    int has_attach;
    struct MessageNode *next;
};

// Estructura para la lista de usuarios registrados
struct UserNode {
    char username[256];
    int status;                 // 0 = desconectado, 1 = conectado
    char ip[INET_ADDRSTRLEN];
    int port;
    unsigned int id_msg;
    struct MessageNode *pending_msgs;
    struct UserNode *next;
};

struct UserNode *users_head = NULL;
pthread_mutex_t users_mutex = PTHREAD_MUTEX_INITIALIZER;


// Función para que el servidor envíe notificaciones a los clientes
int send_to_client(char *ip, int port, char *opcode, char *sender, unsigned int id, char *msg, char *filename) {
    int sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sd < 0) {
        return -1;
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr(ip);

    // Intenta conectar con el hilo de escucha del cliente
    if (connect(sd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sd);
        return -1;
    }
    char buffer_id[32];
    snprintf(buffer_id, sizeof(buffer_id), "%u", id);

    // Si sendMesage falla, ok pasa a 0 y devuelve -1 al final para que el llamante sepa que el msg no se entregó completo
    int ok = 1;
    if (strcmp(opcode, "SEND_MESSAGE") == 0) {
        if (sendMessage(sd, opcode, strlen(opcode) + 1) < 0 ||
            sendMessage(sd, sender, strlen(sender) + 1) < 0 ||
            sendMessage(sd, buffer_id, strlen(buffer_id) + 1) < 0 ||
            sendMessage(sd, msg, strlen(msg) + 1) <0) {
                ok = 0;
            }
    }
    else if (strcmp(opcode, "SEND_MESS_ACK") == 0) {
        if (sendMessage(sd, opcode, strlen(opcode) + 1) < 0 ||
            sendMessage(sd, buffer_id, strlen(buffer_id) + 1) < 0) {
                ok = 0;
            }
    }
    else if (strcmp(opcode, "SEND_MESSAGE_ATTACH") == 0) {
        if (sendMessage(sd, opcode, strlen(opcode) + 1) < 0 ||
            sendMessage(sd, sender, strlen(sender) + 1) < 0 ||
            sendMessage(sd, buffer_id, strlen(buffer_id) + 1) < 0 ||
            sendMessage(sd, msg, strlen(msg) + 1) <0 ||
            sendMessage(sd, filename, strlen(filename) + 1) < 0) {
            ok = 0;
        }
    }
    else if (strcmp(opcode, "SEND_MESS_ATTACH_ACK") == 0) {
        if (sendMessage(sd, opcode, strlen(opcode) + 1) < 0 ||
            sendMessage(sd, buffer_id, strlen(buffer_id) + 1) < 0 ||
            sendMessage(sd, filename, strlen(filename) + 1) < 0) {
            ok = 0;
        }
    }

    close(sd);
    if (ok) {
        return 0;   // éxito
    } else {
        return -1;  // fallo
    }
}

// Función auxiliar para enviar la respuesta al cliente
void enviar_respuesta_byte(int sd, uint8_t res_code) {
    write(sd, &res_code, sizeof(uint8_t));
}


// Función que ejecutará cada hilo para atender a un cliente
void *tratar_peticion(void *arg) {
    // Recuperamos el socket y liberamos la memoria reservada en el main
    int sd = *((int *)arg);
    free(arg);

    char opcode[MAX_LINE];
    char username[MAX_LINE];

    // (1) Leer el Opcode
    if (readLine(sd, opcode, MAX_LINE) <= 0) {
        close(sd);
        pthread_exit(NULL);
    }


    // (2) Procesar según la operación solicitada
    // REGISTER
    if (strcmp(opcode, "REGISTER") == 0) {
        // Leer el nombre de usuario
        if (readLine(sd, username, MAX_LINE) <= 0) {
            close(sd);
            pthread_exit(NULL);
        }
        log_to_RPC(username, "REGISTER", NULL);
        uint8_t res_code = 1;           // Por defecto ERROR (1): el usuario ya existe
        int user_exist = 0;
        struct UserNode *current;

        pthread_mutex_lock(&users_mutex);

        // Comprobar si el usuario ya existe
        current = users_head;
        while (current != NULL) {
            if (strcmp(current->username, username) == 0) {
                user_exist = 1;
                break;
            }
            current = current->next;
        }
        if (!user_exist) {
            // OK: Insertar al principio de la lista (0)
            struct UserNode *new_node = (struct UserNode *)malloc(sizeof(struct UserNode));
            strcpy(new_node->username, username);
            new_node->status = 0;
            new_node->id_msg = 0;
            new_node->pending_msgs = NULL;
            new_node->next = users_head;
            users_head = new_node;
            res_code = 0;
        } 
        pthread_mutex_unlock(&users_mutex);

        // Enviar respuesta al cliente
        enviar_respuesta_byte(sd, res_code);

        // Imprimir logs
        if (res_code == 0) {
            printf("REGISTER %s OK\ns> ", username);
        }
        else {
            printf("REGISTER %s FAIL\ns> ", username);
        }
        fflush(stdout);
    }


    // UNREGISTER
    else if (strcmp(opcode, "UNREGISTER") == 0) {
        // Leer el nombre de usuario
        if (readLine(sd, username, MAX_LINE) <= 0) {
            close(sd);
            pthread_exit(NULL);
        }
        log_to_RPC(username, "UNREGISTER", NULL);
        uint8_t res_code = 1;           // Por defecto ERROR (1): El usuairo no existe
        struct UserNode *current;
        struct UserNode *prev = NULL;

        // Buscar y eliminar
        pthread_mutex_lock(&users_mutex);
        current = users_head;

        while (current != NULL) {
            if (strcmp(current->username, username) == 0) {
                // Usuario encontrado, se desengancha de la lista
                if (prev == NULL) {
                    // Si era el primer nodo
                    users_head = current->next;
                }
                else {
                    prev->next = current->next;
                }
                // Borrar mensajes pendientes
                struct MessageNode *m = current->pending_msgs;
                while (m != NULL) {
                    struct MessageNode *aux = m->next;
                    free(m);
                    m = aux;
                }
                free(current);

                res_code = 0;       // OK (0)
                break;
            }
            prev = current;
            current = current->next;
        }
        pthread_mutex_unlock(&users_mutex);

        // Enviar respuesta al cliente
        enviar_respuesta_byte(sd, res_code);

        // Imprimir logs
        if (res_code == 0) {
            printf("UNREGISTER %s OK\ns> ", username);
        }
        else {
            printf("UNREGISTER %s FAIL\ns> ", username);
        }
        fflush(stdout);
    }


    // CONNECT
    else if (strcmp(opcode, "CONNECT") == 0) {
        char port_str[MAX_LINE];

        // Leer usuario y puerto
        if (readLine(sd, username, MAX_LINE) <= 0 || readLine(sd, port_str, MAX_LINE) <= 0) {
            close(sd);
            pthread_exit(NULL);
        }
        log_to_RPC(username, "CONNECT", NULL);
    
        char *endptr;
        long client_port_l = strtol(port_str, &endptr, 10);
        if (endptr == port_str || *endptr != '\0' || client_port_l <= 0 || client_port_l > 65535) {
            // Error: el número del puerto no es válido 
            enviar_respuesta_byte(sd, 3);
            close(sd);
            pthread_exit(NULL);
        }
        int client_port = (int)client_port_l;

        // Extraer la IP directamente del socket TCP
        struct sockaddr_in peer_addr;
        socklen_t peer_len = sizeof(peer_addr);
        char client_ip[INET_ADDRSTRLEN] = "";

        if (getpeername(sd, (struct sockaddr*)&peer_addr, &peer_len) == 0){
            inet_ntop(AF_INET, &peer_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        }
        uint8_t res_code = 3;                   // Por defecto se asume ERROR (3)
        struct MessageNode *pending = NULL;     // Para extraer los mensajes atrasados

        // Buscar usuario y actualizar
        pthread_mutex_lock(&users_mutex);
        struct UserNode *current = users_head;
        while (current != NULL) {
            if (strcmp(current->username, username) == 0) {
                if (current->status == 1) {
                    res_code = 2;       // ERROR (2): Ya estaba conectado
                }
                else {
                    current->status = 1;
                    current->port = client_port;
                    strcpy(current->ip, client_ip);
                    res_code = 0;       // OK (0)

                    // Guarda lista de pendientes y la vacía del usuario
                    pending = current->pending_msgs;
                    current->pending_msgs = NULL;
                }
                break;
            }
            current = current->next;
        }
        if (current == NULL) {
            res_code = 1;               // ERROR (1): El usuario no existe
        }
        pthread_mutex_unlock(&users_mutex);

        // Enviar respuesta al cliente
        enviar_respuesta_byte(sd, res_code);

        // Imprimir logs
        if (res_code == 0) {
            printf("CONNECT %s OK\ns> ", username);
        }
        else {
            printf("CONNECT %s FAIL\ns> ", username);
        }
        fflush(stdout);

        // Lógica de mensajes pendientes
        if (res_code == 0 && pending != NULL) {
            struct MessageNode *m = pending;
            while (m != NULL) {
                int delivery_ok;

                // Intentar enviar mensaje almacenado con archivo adjunto
                if (m->has_attach) {
                    delivery_ok = send_to_client(client_ip, client_port, "SEND_MESSAGE_ATTACH", m->sender, m->id, m->message, m->filename);
                }
                // Intentar enviar mensaje almacenado normal
                else {
                    delivery_ok = send_to_client(client_ip, client_port, "SEND_MESSAGE", m->sender, m->id, m->message, "");
                }

                // Si se entrega, envia ACK y libera
                if (delivery_ok == 0) {
                    printf("SEND MESSAGE %u FROM %s TO %s\ns> ", m->id, m->sender, username);
                    fflush(stdout);

                    // Buscar si el remitente está conectado para mandarle el ACK
                    pthread_mutex_lock(&users_mutex);
                    struct UserNode *s_curr = users_head;
                    char s_ip[INET_ADDRSTRLEN];
                    int s_port = 0;
                    int send_ack = 0;
                    while (s_curr != NULL) {
                        if (strcmp(s_curr->username, m->sender) == 0 && s_curr->status == 1) {
                            strcpy(s_ip, s_curr->ip);
                            s_port = s_curr->port;
                            send_ack = 1;
                            break;
                        }
                        s_curr = s_curr->next;
                    }
                    pthread_mutex_unlock(&users_mutex);

                    // Mandar el ACK correspondiente
                    if (send_ack) {
                        if (m->has_attach) {
                            send_to_client(s_ip, s_port, "SEND_MESS_ATTACH_ACK", "", m->id, "", m->filename);
                        }
                        else {
                            send_to_client(s_ip, s_port, "SEND_MESS_ACK", "", m->id, "", "");
                        }   
                    }
                    // Liberar la memoria
                    struct MessageNode *aux = m;
                    m = m->next;
                    free(aux);
                }

                // Si falló el envío, reinsertar mensaje en la cola de mensajes pendientes
                else {
                    struct MessageNode *failed_msg = m;
                    m = m->next;
                    failed_msg->next = NULL;

                    pthread_mutex_lock(&users_mutex);
                    struct UserNode *u_curr = users_head;
                    while (u_curr != NULL) {
                        if (strcmp(u_curr->username, username) == 0) {
                            // Reinserat al final de la lista
                            if (u_curr->pending_msgs == NULL) {
                                u_curr->pending_msgs = failed_msg;
                            }
                            else {
                                struct MessageNode *aux = u_curr->pending_msgs;
                                while (aux->next) {
                                    aux = aux->next;
                                }
                                aux->next = failed_msg;
                            }
                            break;
                        }
                        u_curr = u_curr->next;
                    }
                    pthread_mutex_unlock(&users_mutex);
                }         
            }
        }
    }


    // DISCONNECT
    else if (strcmp(opcode, "DISCONNECT") == 0) {
        uint8_t res_code = 3;          // Por defecto se asume ERROR (3)

        // Leer usuario y puerto
        if (readLine(sd, username, MAX_LINE) <= 0) {
            close(sd);
            pthread_exit(NULL);
        }
        log_to_RPC(username, "DISCONNECT", NULL);

        // Extraer la IP directamente del socket TCP
        struct sockaddr_in peer_addr;
        socklen_t peer_len = sizeof(peer_addr);
        char client_ip[INET_ADDRSTRLEN] = "";

        if (getpeername(sd, (struct sockaddr*)&peer_addr, &peer_len) == 0){
            inet_ntop(AF_INET, &peer_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        }
        struct UserNode *current;
        pthread_mutex_lock(&users_mutex);
        current = users_head;

        // Buscar usuario y actualizar
        while (current != NULL) {
            if (strcmp(current->username, username) == 0) {
                if (current->status == 0) {
                    res_code = 2;       // ERROR (2): No está conectado
                }
                else if (strcmp(current->ip, client_ip) != 0) {
                    res_code = 3;       // ERROR (3): La IP no es la que se conectó
                }
                else {
                    current->status = 0;
                    strcpy(current->ip, "");
                    current->port = 0;
                    res_code = 0;       // OK (0)
                }
                break;
            }
            current = current->next;
        }
        if (current == NULL) {
            res_code = 1;               // ERROR (1): El usuario no existe
        }
        pthread_mutex_unlock(&users_mutex);

        // Enviar respuesta al cliente
        enviar_respuesta_byte(sd, res_code);

        // Imprimir logs
        if (res_code == 0) {
            printf("DISCONNECT %s OK\ns> ", username);
        }
        else {
            printf("DISCONNECT %s FAIL\ns> ", username);
        }
        fflush(stdout);
    }


    // SEND
    else if (strcmp(opcode, "SEND") == 0) {
        char sender[MAX_LINE], dest[MAX_LINE], msg_text[MAX_LINE];

        // Leer remitente, destinatario y mensaje
        if (readLine(sd, sender, MAX_LINE) <= 0 || 
            readLine(sd, dest, MAX_LINE) <= 0 || 
            readLine(sd, msg_text, MAX_LINE) <= 0) {
            close(sd);
            pthread_exit(NULL);
        }
        log_to_RPC(sender, "SEND", NULL);
        uint8_t res_code = 2;       // Por defecto ERROR (2)
        unsigned int new_id = 0;

        pthread_mutex_lock(&users_mutex);
        struct UserNode *s_node = NULL;
        struct UserNode *d_node = NULL;
        struct UserNode *curr = users_head;
        
        // Buscar a ambos
        while (curr != NULL) {
            if (strcmp(curr->username, sender) == 0) {
                s_node = curr;
            }
            if (strcmp(curr->username, dest) == 0) {
                d_node = curr;
            }
            curr = curr->next;
        }

        if (s_node == NULL || d_node == NULL) {
            res_code = 1;       // ERROR (1): Alguno no existe
            pthread_mutex_unlock(&users_mutex);
            enviar_respuesta_byte(sd, res_code);
            close(sd);
            pthread_exit(NULL);
        }

        // Generar ID y controlar desbordamiento
        s_node->id_msg += 1;
        if (s_node->id_msg == 0) {
            s_node->id_msg = 1;     // Si desborda vuelve a 1
        }
        new_id = s_node->id_msg;

        // Almacenar mensaje en la cola del destinatario
        struct MessageNode *m_node = malloc(sizeof(struct MessageNode));
        strcpy(m_node->sender, sender);
        m_node->id = new_id;
        strcpy(m_node->message, msg_text);
        strcpy(m_node->filename, "");
        m_node->has_attach = 0;
        m_node->next = NULL;

        if (d_node->pending_msgs == NULL) {
            d_node->pending_msgs = m_node;
        }
        else {
            struct MessageNode *aux = d_node->pending_msgs;
            while(aux->next) {
                aux = aux->next;
            }
            aux->next = m_node;
        }

        // Copia de datos para la red (para hacerlo fuera del mutex)
        int d_status = d_node->status;
        char d_ip[INET_ADDRSTRLEN];
        strcpy(d_ip, d_node->ip);
        int d_port = d_node->port;
        res_code = 0;
        pthread_mutex_unlock(&users_mutex);

        // Enviar respuesta al cliente (Código 0 e ID)
        enviar_respuesta_byte(sd, res_code);
        if (res_code == 0) {
            char id_str[32];
            sprintf(id_str, "%u", new_id);
            sendMessage(sd, id_str, strlen(id_str) + 1);
        }

        // Intentar entrega si el destinatario está conectado
        if (d_status == 1) {
            if (send_to_client(d_ip, d_port, "SEND_MESSAGE", sender, new_id, msg_text, "") == 0) {
                printf("SEND MESSAGE %u FROM %s TO %s\ns> ", new_id, sender, dest);
                fflush(stdout);

                // Si se entrega, se borra el mensaje de la cola
                pthread_mutex_lock(&users_mutex);
                curr = users_head;
                while (curr && strcmp(curr->username, dest) != 0) {
                    curr = curr->next;
                }
                if (curr) {
                    struct MessageNode *prev_m = NULL;
                    struct MessageNode *m = curr->pending_msgs;
                    while(m) {
                        if (m->id == new_id && strcmp(m->sender, sender) == 0) {
                            if (prev_m) {
                                prev_m->next = m->next;
                            }
                            else {
                                curr->pending_msgs = m->next;
                            }
                            free(m);
                            break;
                        }
                        prev_m = m;
                        m = m->next;
                    }
                }
                // Mira si el remitente sigue conectado para enviarle el ACK
                struct UserNode *s_curr = users_head;
                while (s_curr && strcmp(s_curr->username, sender) != 0) {
                    s_curr = s_curr->next;
                }
                int send_ack = (s_curr && s_curr->status == 1);
                char s_ip[INET_ADDRSTRLEN];
                int s_port = 0;
                if (send_ack) {
                    strcpy(s_ip, s_curr->ip); 
                    s_port = s_curr->port;
                }
                pthread_mutex_unlock(&users_mutex);

                // Manda el ACK
                if (send_ack) {
                    send_to_client(s_ip, s_port, "SEND_MESS_ACK", "", new_id, "", "");
                }
            }
            else {
                // Fallo de red con destinatario (se marca como si estuviera desconectado)
                pthread_mutex_lock(&users_mutex);
                curr = users_head;
                while(curr && strcmp(curr->username, dest) != 0) {
                    curr = curr->next;
                }
                if (curr) {
                    curr->status = 0;
                }
                pthread_mutex_unlock(&users_mutex);
                printf("MESSAGE %u FROM %s TO %s STORED\ns> ", new_id, sender, dest);
                fflush(stdout);
            }
        }
        else {
            // Destinatario desconectado, imprime STORED
            printf("MESSAGE %u FROM %s TO %s STORED\ns> ", new_id, sender, dest);
            fflush(stdout);
        }
    }


    // SENDATTACH
    else if (strcmp (opcode, "SENDATTACH") == 0) {
        char sender[MAX_LINE], dest[MAX_LINE], msg_text[MAX_LINE], fname[MAX_LINE];

        // Leer remitente, destinatario, mensaje  y nombre del fichero
        if (readLine(sd, sender, MAX_LINE) <= 0 || 
            readLine(sd, dest, MAX_LINE) <= 0 || 
            readLine(sd, msg_text, MAX_LINE) <= 0 ||
            readLine(sd, fname, MAX_LINE) <= 0) {
            close(sd);
            pthread_exit(NULL);
        }
        log_to_RPC(sender, "SENDATTACH", fname);
        uint8_t res_code = 2;       // Por defecto ERROR (2)
        unsigned int new_id = 0;

        pthread_mutex_lock(&users_mutex);
        struct UserNode *s_node = NULL;
        struct UserNode *d_node = NULL;
        struct UserNode *curr = users_head;
        
        // Buscar a ambos usuarios
        while (curr != NULL) {
            if (strcmp(curr->username, sender) == 0) {
                s_node = curr;
            }
            if (strcmp(curr->username, dest) == 0) {
                d_node = curr;
            }
            curr = curr->next;
        }

        if (s_node == NULL || d_node == NULL) {
            res_code = 1;       // ERROR (1): Alguno no existe
            pthread_mutex_unlock(&users_mutex);
            enviar_respuesta_byte(sd, res_code);
            close(sd);
            pthread_exit(NULL);
        }

        // Generar ID y controlar desbordamiento
        s_node->id_msg += 1;
        if (s_node->id_msg == 0) {
            s_node->id_msg = 1;     // Si desborda vuelve a 1
        }
        new_id = s_node->id_msg;

        // Almacenar mensaje con fichero adjunto en la cola del destinatario
        struct MessageNode *m_node = malloc(sizeof(struct MessageNode));
        strcpy(m_node->sender, sender);
        m_node->id = new_id;
        strcpy(m_node->message, msg_text);
        strcpy(m_node->filename, fname);
        m_node->has_attach = 1;
        m_node->next = NULL;

        if (d_node->pending_msgs == NULL) {
            d_node->pending_msgs = m_node;
        }
        else {
            struct MessageNode *aux = d_node->pending_msgs;
            while(aux->next) {
                aux = aux->next;
            }
            aux->next = m_node;
        }

        // Copia de datos para la red (para hacerlo fuera del mutex)
        int d_status = d_node->status;
        char d_ip[INET_ADDRSTRLEN];
        strcpy(d_ip, d_node->ip);
        int d_port = d_node->port;
        res_code = 0;
        pthread_mutex_unlock(&users_mutex);

        // Enviar respuesta al cliente (Código 0 e ID)
        enviar_respuesta_byte(sd, res_code);
        if (res_code == 0) {
            char id_str[32];
            sprintf(id_str, "%u", new_id);
            sendMessage(sd, id_str, strlen(id_str) + 1);
        }

        // Intentar entrega si el destinatario está conectado
        if (d_status == 1) {
            if (send_to_client(d_ip, d_port, "SEND_MESSAGE_ATTACH", sender, new_id, msg_text, fname) == 0) {
                printf("SEND MESSAGE %u FROM %s TO %s\ns> ", new_id, sender, dest);
                fflush(stdout);

                // Si se entrega, se borra el mensaje de la cola
                pthread_mutex_lock(&users_mutex);
                curr = users_head;
                while (curr && strcmp(curr->username, dest) != 0) {
                    curr = curr->next;
                }
                if (curr) {
                    struct MessageNode *prev_m = NULL;
                    struct MessageNode *m = curr->pending_msgs;
                    while(m) {
                        if (m->id == new_id && strcmp(m->sender, sender) == 0) {
                            if (prev_m) {
                                prev_m->next = m->next;
                            }
                            else {
                                curr->pending_msgs = m->next;
                            }
                            free(m);
                            break;
                        }
                        prev_m = m;
                        m = m->next;
                    }
                }
                // Mira si el remitente sigue conectado para enviarle el ACK
                struct UserNode *s_curr = users_head;
                while (s_curr && strcmp(s_curr->username, sender) != 0) {
                    s_curr = s_curr->next;
                }
                int send_ack = (s_curr && s_curr->status == 1);
                char s_ip[INET_ADDRSTRLEN];
                int s_port = 0;
                if (send_ack) {
                    strcpy(s_ip, s_curr->ip); 
                    s_port = s_curr->port;
                }
                pthread_mutex_unlock(&users_mutex);

                // Manda el ACK con el nombre del fichero
                if (send_ack) {
                    send_to_client(s_ip, s_port, "SEND_MESS_ATTACH_ACK", "", new_id, "", fname);
                }
            }
            else {
                // Fallo de red con destinatario (se marca como si estuviera desconectado)
                pthread_mutex_lock(&users_mutex);
                curr = users_head;
                while(curr && strcmp(curr->username, dest) != 0) {
                    curr = curr->next;
                }
                if (curr) {
                    curr->status = 0;
                }
                pthread_mutex_unlock(&users_mutex);
                printf("MESSAGE %u FROM %s TO %s STORED\ns> ", new_id, sender, dest);
                fflush(stdout);
            }
        }
        else {
            // Destinatario desconectado, imprime STORED
            printf("MESSAGE %u FROM %s TO %s STORED\ns> ", new_id, sender, dest);
            fflush(stdout);
        }
    }


    // USERS
    else if (strcmp (opcode, "USERS") == 0) {
        // Leer el nombre de usuario que hace la petición
        if (readLine (sd, username, MAX_LINE) <= 0) {
            close(sd);
            pthread_exit(NULL);
        }
        log_to_RPC(username, "USERS", NULL);
        uint8_t res_code = 2;       // Por defecto ERROR (2)

        // Conectar usuarios y verificar al solicitante
        char **connected_users = NULL;
        int n_connected = 0;
        int capacity = 0;
        int user_found = 0;
        int user_connected = 0;

        pthread_mutex_lock(&users_mutex);
        struct UserNode *current = users_head;

        // Verificar que el usuario que lo pide existe y está conectado
        while (current != NULL) {
            if (strcmp(current->username, username) == 0) {
                user_found = 1;
                if (current->status == 1) {
                    user_connected = 1;
                }
            }
            current = current->next;
        }

        // Si está conectado, recopilar la lista de usuarios conectados
        if (user_found && user_connected) {
            current = users_head;
            while (current != NULL) {
                if (current->status == 1) {
                    // Crece el array dinámicamente (sin límite de usuarios)
                    if (n_connected >= capacity) {
                        if (capacity == 0) {
                            capacity = 8;
                        } else {
                            capacity = capacity * 2;
                        }
                        char **tmp = realloc(connected_users, capacity * sizeof(char *));
                        if (tmp == NULL) {
                            // Si falla realloc, libera lo reservado y aborta con error
                            for (int i = 0; i < n_connected; i++) {
                                free(connected_users[i]);
                            }
                            free(connected_users);
                            pthread_mutex_unlock(&users_mutex);
                            enviar_respuesta_byte(sd, 2);
                            printf("CONNECTED USERS FAIL\ns> ");
                            fflush(stdout);
                            close(sd);
                            pthread_exit(NULL);
                        }
                        connected_users = tmp;
                    }
                    // Reservar espacio para esta cadena (username + IP + puerto + separadores)
                    connected_users[n_connected] = malloc(512);
                    if (connected_users[n_connected] == NULL) {
                        for (int i = 0; i < n_connected; i++) {
                            free(connected_users[i]);
                        }
                        free(connected_users);
                        pthread_mutex_unlock(&users_mutex);
                        enviar_respuesta_byte(sd, 2);
                        printf("CONNECTED USERS FAIL\ns> ");
                        fflush(stdout);
                        close(sd);
                        pthread_exit(NULL);
                    }
                    snprintf(connected_users[n_connected], 512, "%s::%s::%d", current->username, current->ip, current->port);
                    n_connected++;
                }
                current = current->next;
            }
            res_code = 0;       // OK (0)
        }
        else if (user_found && !user_connected) {
            res_code = 1;       // ERROR (1): El usuario no está conectado
        }
        else {
            res_code = 2;       // ERROR (2): El usuario no existe
        }
        pthread_mutex_unlock(&users_mutex);

        // Enviar respuesta al cliente
        enviar_respuesta_byte(sd, res_code);

        // Si todo es correcto, enviar número de usuarios y sus nombres
        if (res_code == 0) {
            char n_str[32];
            sprintf(n_str, "%d", n_connected);
            sendMessage(sd, n_str, strlen(n_str) + 1);

            for (int i = 0; i < n_connected; i++) {
                sendMessage(sd, connected_users[i], strlen(connected_users[i]) + 1);
            }
            printf("CONNECTEDUSERS OK\ns> ");
        }
        else {
            printf("CONNECTEDUSERS FAIL\ns> ");
        }
        fflush(stdout);

        // Liberar la memoria reservada para la ista de usuarios conectados
        for (int i = 0; i < n_connected; i++) {
            free(connected_users[i]);
        }
        free(connected_users);
    }


    // OPERACIÓN DESCONOCIDA
    else {
        printf("ERROR: OPERACIÓN DESCONOCIDA %s\ns> ", opcode);
        enviar_respuesta_byte(sd, 2);       // ERROR
    }
    

    // (3) Cerrar la conexión con el cliente y matar el hilo
    close(sd);
    pthread_exit(NULL);
}


int main(int argc, char *argv[]) {
    // Ignorar SIGPIPE para evitar que el servicio muera si un  
    // cliente cierra la conexión a destiempo durante un envío
    signal(SIGPIPE, SIG_IGN);
    
    int sd, *newsd;
    int val, ret;
    struct sockaddr_in server_addr, client_addr;
    socklen_t size;
    pthread_attr_t t_attr;
    pthread_t thid;

    // Puerto por defecto 8080
    int port = 8080;
    if (argc == 3 && strcmp(argv[1], "-p") == 0) {
        char *endptr;
        long port_l = strtol(argv[2], &endptr, 10);
        if (endptr == argv[2] || *endptr != '\0' || port_l < 1024 || port_l > 65535) {
            printf("Error: el puerto debe ser un número entre 1024 y 65535\n");
            exit(-1);
        }
        port = (int)port_l;
    } else {
        printf("Uso: %s -p <puerto>\n", argv[0]);
        exit(-1);
    }


    // (1) Crear el socket
    sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sd < 0) {
        perror("Error in socket");
        exit(1);
    }

    // Evita el error "dirección ya está en uso" si matamos y reiniciamos el servidor
    val = 1;
    ret = setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, (char *) &val, sizeof(int));
    if (ret < 0) {
        perror("Error in option");
        return -1;
    }

    // (2) Obtener la dirección
    bzero((char *)&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    // (3) bind
    ret = bind(sd, (struct sockaddr *)&server_addr, sizeof(server_addr));
    if (ret < 0){
        perror("Error en bind: ");
        return -1;
    }

    // Obtener la IP local dinámica
    char local_ip[INET_ADDRSTRLEN];
    char hostname[256];
    struct hostent *he;

    if (gethostname(hostname, sizeof(hostname)) == -1) {
        perror("Error en getsockname");
        strcpy(local_ip, "0.0.0.0");
    }
    else {
        he = gethostbyname(hostname);
        if (he == NULL) {
            perror("Error en gethostbyname");
            strcpy(local_ip, "0.0.0.0");
        }
        else {
            inet_ntop(AF_INET, he->h_addr_list[0], local_ip, sizeof(local_ip));
        }
    }

    // (4) listen
    ret = listen(sd, SOMAXCONN);
    if (ret < 0) {
        perror("Error en listen: ");
        return -1;
    }

    printf("s> init server %s:%d\n", local_ip, port);
    printf("s> ");
    fflush(stdout);

    pthread_attr_init(&t_attr);
    pthread_attr_setdetachstate(&t_attr, PTHREAD_CREATE_DETACHED);

    while(1) {
        // accept
        size = sizeof(struct sockaddr_in);
        bzero(&client_addr, size);

        // usamos malloc para evitar condiciones de carrera entre hilos
        newsd = malloc (sizeof(int));
        if (newsd == NULL) {
            perror("Error en malloc");
            continue;
        }

        *newsd = accept (sd, (struct sockaddr *)&client_addr, &size);
        if (*newsd < 0) {
            perror("Error en el accept");
            free(newsd);
            continue;
        }

        // Crear el hilo trabajador
        if (pthread_create(&thid, &t_attr, tratar_peticion, (void *)newsd) != 0) {
            perror("Error al crear el hilo");
            close(*newsd);
            free(newsd);
        }

    }

    // close server socket
    close(sd);
    pthread_attr_destroy(&t_attr);
    return(0);    
}
