### 1. Create and Activate Python Virtual Environment

```bash
# Create a Python virtual environment
uv venv .venv --python 3.11

# Activate the virtual environment
source .venv/bin/activate
```

### 2. Configure Airflow Environment

```bash
# Set environment variables
export AIRFLOW_HOME="$(pwd)"/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)"/dags

# Initialize the Airflow database
airflow db migrate
```

### 3. Create Admin User

```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

### 4. Start Airflow Services

Open two terminal windows to run these services:

```bash
# Terminal 1: Start the Airflow scheduler
airflow scheduler
```

```bash
# Terminal 2: Start the Airflow webserver
airflow webserver --port 8080
```

Once started, access the Airflow web interface at http://localhost:8080
