### 1. Create and Activate Python Virtual Environment
```bash
uv venv .venv --python 3.11

source .venv/bin/activate
```

### 2. Install requirements

```bash
uv pip install -r requirements.txt
```

### 3. Configure environment variables

```bash
export AIRFLOW_HOME="$(pwd)"/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)"/dags
```

### 4. Create airflow database and create Admin User

```bash
airflow db migrate

airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

### 5. Start Airflow scheduler and webserver

```bash
airflow scheduler
```

```bash
airflow webserver --port 8080
```

Once started, access the Airflow web interface at http://localhost:8080
