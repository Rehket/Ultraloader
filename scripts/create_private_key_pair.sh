

openssl genpkey -des3 -algorithm RSA -pass pass:$PKEY_PASS -out server.pass.key -pkeyopt rsa_keygen_bits:$PKEY_BITS

#openssl rsa -passin pass:$PKEY_PASS -in server.pass.key -out server.key
#
#openssl req -new -key server.key -out server.csr -subj "/C=$PKEY_COUNTRY/ST=$PKEY_STATE/L=$PKEY_CITY/O=$PKEY_COMPANY/OU=$PKEY_DEPARTMENT/CN=$PKEY_DOMAIN_NAME"
#
#openssl x509 -req -sha256 -days 365 -in server.csr -signkey server.key -out server.crt