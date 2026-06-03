struct log_arg {
    char username[256];
    char operation[32];
    char filename[256];
};

program LOG {
    version LOG_VERSION {
        int rpc_log(log_arg) = 1;
    } = 1;
} = 100522214;