#!/bin/bash
# Inicia o robô em segundo plano
cd /home/usuario/backend
python ROBO_cripto_parte_1.py &

# Inicia o dashboard em segundo plano
cd /home/usuario/backend/dashboard
python server.py &

# Mantém o script rodando
wait