FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

# --threads: gunicorn's sync workers hold one request for its ENTIRE
# duration — including the AI assistant's streamed chat responses, which can
# run for tens of seconds per step. With plain sync workers, a handful of
# concurrent chats would fully occupy every worker and block ALL other
# traffic (login, uploads, everything) on that container. --threads lets
# each of the 4 worker processes serve several requests concurrently instead.
# --timeout: gunicorn's default is 30s — comfortably enough for every other
# route here, but far too short for an LLM response (services/llm_service.py
# waits up to Config.LLM_TIMEOUT_SECONDS, 300s by default); gunicorn would
# otherwise kill the worker mid-stream and the browser sees a bare
# connection failure. Keep this >= LLM_TIMEOUT_SECONDS if you change it.
CMD ["gunicorn", "-w", "4", "--threads", "8", "--timeout", "300", "-b", "0.0.0.0:5000", "app:create_app()"]
