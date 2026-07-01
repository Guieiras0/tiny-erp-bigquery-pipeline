# Usa uma imagem leve do Python
FROM python:3.11-slim

# --- MELHORIA 1: FORÇAR LOGS EM TEMPO REAL ---
# Isso garante que cada 'print' ou 'info' apareça no Google Cloud instantaneamente
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Define a pasta de trabalho
WORKDIR /app

# Copia e instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todos os seus scripts (.py) para a imagem
COPY . .

# Comando padrão (pode ser sobrescrito pelo Job)
CMD ["python", "append_pai.py"]
