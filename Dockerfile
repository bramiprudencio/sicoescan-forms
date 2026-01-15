# Usamos Python 3.10 versión "slim" (más ligera y segura)
FROM python:3.10-slim

# Evita que Python guarde archivos .pyc y fuerza que los logs salgan inmediatamente (útil para Cloud Logging)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# 1. Copiamos requirements primero para aprovechar la caché de Docker
COPY requirements.txt .

# 2. Instalamos dependencias
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copiamos el resto del código (main.py, carpetas processors, shared, etc.)
COPY . .

# IMPORTANTE: Cloud Functions necesita exponer el puerto 8080
ENV PORT=8080

# Usamos functions-framework para levantar el servidor
# --target debe coincidir con el nombre de tu función en main.py
CMD ["functions-framework", "--target=router_process"]