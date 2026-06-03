/*
 * Implementación del servicio RPC de log.
 * Cuando el servidor de mensajería envía una operación, este servicio
 * imprime por pantalla el nombre de usuario y la operación.
 */

#include "logRPC.h"
#include <stdio.h>
#include <string.h>

bool_t
rpc_log_1_svc(log_arg a, int *result,  struct svc_req *rqstp)
{
	/* En SENDATTACH se imprime el nombre del fichero adjunto también*/
	if (strcmp(a.operation, "SENDATTACH") == 0 && a.filename[0] != '\0') {
		printf("%s\t%s\t%s\n", a.username, a.operation, a.filename);
	}
	else {
		printf("%s\t%s\n", a.username, a.operation);
	}
	fflush(stdout);

	*result = 0;
	return TRUE;
}

int
log_1_freeresult (SVCXPRT *transp, xdrproc_t xdr_result, caddr_t result)
{
	xdr_free (xdr_result, result);
	return 1;
}
