#LOCAL DOCKERFILE

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app/backend

# Copying my backend and data files
COPY backend /app/backend/ 

# Installing backend package from pyproject.toml
RUN pip install --upgrade pip && pip install --no-cache-dir -e .

EXPOSE 8000

CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]