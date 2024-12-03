import asyncio
from contextlib import asynccontextmanager
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import smtplib
from typing import List
from fastapi import  FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from auth import router as auth_router
from jwt_utils import verify_token
import pandas as pd
import mysql.connector
from mysql.connector import Error
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, time 
import locale
from user import router as user_router
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Scheduler pour les alertes
scheduler = AsyncIOScheduler()

# Define the lifespan_events function first
@asynccontextmanager
async def lifespan_events(app: FastAPI):
    # Startup code
    print("Application startup")
    asyncio.create_task(schedule_async_job()) 
    yield  # This part indicates the running of the application
    # Shutdown code
    print("Application shutdown")
    scheduler.shutdown(wait=False)


# Create the main FastAPI app
app = FastAPI(lifespan=lifespan_events)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permettre toutes les origines (à limiter en production)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include authentication routes under '/auth' to avoid conflicts
app.include_router(auth_router, prefix="/auth")

# Ajouter les routes user
app.include_router(user_router, prefix="/user", tags=["User"])

# Function to connect to MySQL
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host="localhost",          
            database="monitoringsvfe",        
            user="root",      
            password=""   
        )
        if conn.is_connected():
            return conn
    except Error as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")
    

# Function to create necessary tables
def create_tables_if_not_exists():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create 'user' and 'transactions' tables
    create_user_table = """
    CREATE TABLE IF NOT EXISTS user (
        id INT AUTO_INCREMENT PRIMARY KEY,
        password_hash VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        username VARCHAR(255),
        phone INT (255),
        image BLOB
    )
    """
    create_monitoring_table = """
    CREATE TABLE IF NOT EXISTS transactions (
        UDATE DATE,
        TIME TIME,
        ISS_INST VARCHAR(255),
        ACQ_INST VARCHAR(255),
        TERMINAL_TYPE VARCHAR(255),
        RESP INT,
        TRANSX_NUMBER INT
    )
    """
    create_alerts_table = """
    CREATE TABLE IF NOT EXISTS alerts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    message VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
    cursor.execute(create_alerts_table)
    cursor.execute(create_user_table)
    cursor.execute(create_monitoring_table)

    conn.commit()
    cursor.close()
    conn.close()

# Ensure tables are created
create_tables_if_not_exists()

# Example query to load data from 'transactions' table
query = """
    SELECT UDATE, TIME, ISS_INST, ACQ_INST, TERMINAL_TYPE, RESP, TRANSX_NUMBER
    FROM transactions
"""
query1 = """
    SELECT UDATE, TIME, ISS_INST, ACQ_INST, TERMINAL_TYPE, RESP, TRANSX_NUMBER
    FROM transactions_hist1
"""

# Set the locale to French for month names
locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8') 

# Function to load data into a DataFrame
def load_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Create a DataFrame from the rows
    df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
    cursor.close()
    conn.close()

    # Data processing steps
    df['RESP'] = pd.to_numeric(df['RESP'], errors='coerce')
    df['DATETIME'] = pd.to_datetime(
    df['UDATE'].astype(str) + df['TIME'].astype(str),
    format='%Y%m%d:%H%M%S',
    errors='coerce'
    )
    df['SUCCESS'] = df['RESP'].apply(lambda x: 1 if x in [-1, 0] else 0)
    return df

def load_data1():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query1)
    rows = cursor.fetchall()

    # Create a DataFrame from the rows
    df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
    cursor.close()
    conn.close()

    # Data processing steps
    df['RESP'] = pd.to_numeric(df['RESP'], errors='coerce')
    df['DATETIME'] = pd.to_datetime(
        df['UDATE'].astype(str) + df['TIME'].astype(str),
        format='%Y%m%d:%H%M%S',
        errors='coerce'
    )
    df['SUCCESS'] = df['RESP'].apply(lambda x: 1 if x in [-1, 0] else 0)

    # Handle NaN values by filling them with None (JSON-compliant)
    df.fillna(value=pd.NA, inplace=True)
    
    # Optionally, you can also handle infinite values like so:
    df.replace([float('inf'), float('-inf')], None, inplace=True)

    return df


def send_email_alert(body):
    sender_email = "monitoringsmt@gmail.com"  # Remplace par ton email
    password = "hviq zfxg uidm snoo"  # Mot de passe d'application SMTP

    # Configurer le serveur SMTP
    smtp_server = "smtp.gmail.com"
    smtp_port = 587  # Port pour le serveur SMTP

    # Récupérer les emails des utilisateurs
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT email FROM user")
        users = cursor.fetchall()
        cursor.close()
        conn.close()

        if not users:
            print("Aucun utilisateur trouvé dans la base de données.")
            return

        # Envoyer un email à chaque utilisateur
        for user in users:
            receiver_email = user[0]  # Chaque email est le premier élément de chaque ligne
            try:
                msg = MIMEMultipart()
                msg["From"] = sender_email
                msg["To"] = receiver_email
                msg["Subject"] = "Alerte SMTMonitoring"

                # Ajouter le contenu de l'email
                msg.attach(MIMEText(body, "plain"))

                # Envoyer l'email
                with smtplib.SMTP(smtp_server, smtp_port) as server:
                    server.starttls()
                    server.login(sender_email, password)
                    server.sendmail(sender_email, receiver_email, msg.as_string())
                    print(f"Email envoyé avec succès")
            except Exception as e:
                print(f"Erreur lors de l'envoi")

    except Error as e:
        print(f"Erreur lors de la récupération des emails : {e}")




async def check_and_send_alerts():
    print("Check and send alerts is running...")
    try:
        # Charger les données pour vérifier les KPIs
        df = load_data()

        total_transactions = len(df)
        successful_transactions = df['SUCCESS'].sum()
        success_rate = (successful_transactions / total_transactions) * 100
        refused_transactions = len(df[df['RESP'] != -1])
        refusal_rate = (refused_transactions / total_transactions) * 100

        response_distribution = df[(df['RESP'] != -1) & (df['RESP'] != 0)]['RESP'].value_counts()
        most_frequent_refusal_code = response_distribution.idxmax() if not response_distribution.empty else None
        most_frequent_refusal_count = response_distribution.max() if not response_distribution.empty else 0

        # Vérification des alertes
        critical_response_codes = [802, 803, 910, 915]

        if success_rate < 70:
            message = f"Alerte Critique: Le taux de reussite est tombe a {success_rate:.2f}%!"
            #await manager.broadcast(message)
            log_alert(message)
            send_email_alert(f"{message}\n\n"
                "Merci de vérifier l'état du système à partir de l'application SMTMonitoring.")


        if refusal_rate > 35:
            message = f"Alerte: Le taux de refus est eleve a {refusal_rate:.2f}%!"
            #await manager.broadcast(message)
            log_alert(message)

        if most_frequent_refusal_code in critical_response_codes:
            message = (
                f"Alerte Critique: Code de refus frequent {most_frequent_refusal_code} "
                f"avec {most_frequent_refusal_count} occurrences!"
            )
            #await manager.broadcast(message)
            log_alert(message)

    except Exception as e:
        print(f"Erreur lors de la vérification des alertes : {e}")


# Function to schedule the async job
# This is how to add async jobs properly in an async context
async def schedule_async_job():
    if not scheduler.get_jobs():
        scheduler.add_job(check_and_send_alerts, 'interval', minutes=1)
    # Start the scheduler (it runs as part of the asyncio event loop)
    scheduler.start()

def log_alert(message):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO alerts (message) VALUES (%s)", (message,))
        conn.commit()
        # Diffuser l'alerte via WebSocket
        asyncio.create_task(manager.broadcast(message))
        cursor.close()
        conn.close()
        #print(f"Broadcasting message: {message}")

    except Error as e:
        print(f"Erreur lors de l'enregistrement de l'alerte : {e}")

# Simuler une tâche périodique
def task_to_check_and_notify():
    print(f"Vérification effectuée à {datetime.now()}")

    # Simulation d'une alerte à envoyer
    message = {
        "type": "alerte",
        "time": str(datetime.now()),
        "details": "Un événement critique a été détecté.",
    }

    # Envoi via WebSocket
    asyncio.run(send_notification(message))

    async def send_notification(message: dict):
        async with WebSocket("ws://localhost:8000/ws/notifications") as websocket:
            await websocket.send_json(message)

    # Ajouter la tâche au planificateur
    scheduler.every(1).minutes.do(task_to_check_and_notify)

    # Boucle pour exécuter les tâches planifiées
    while True:
        scheduler.run_pending()
        time.sleep(1)

@app.get("/alerts/")
def get_alerts(limit: int = 10, authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    
    token = authorization.split(" ")[1]
    verify_token(token)

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM alerts ORDER BY created_at DESC LIMIT %s", (limit,)
        )
        alerts = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"alerts": alerts}
    except Error as e:
        print(f"Erreur lors de la récupération des alertes : {e}")
        raise HTTPException(status_code=500, detail="Erreur de récupération des alertes")



# Transactions route (no conflict now)
@app.get("/transactions/")
def get_transactions(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    
    token = authorization.split(" ")[1]
    verify_token(token)

    df = load_data()
    return df.to_dict(orient='records')

@app.get("/transactions_hist/")
def get_transactions_hist(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    
    token = authorization.split(" ")[1]
    verify_token(token)

    df = load_data1()

    # Convert the DataFrame to a dictionary and return it as a JSON response
    return json.loads(df.to_json(orient='records'))

@app.get("/kpis/")
def get_kpis(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
   
    token = authorization.split(" ")[1]
    verify_token(token)
 
    df = load_data()
 
    total_transactions = len(df)
    successful_transactions = df['SUCCESS'].sum()
    success_rate = (successful_transactions / total_transactions) * 100
    refused_transactions = len(df[df['RESP'] != -1])
    refusal_rate = (refused_transactions / total_transactions) * 100
 
    response_distribution = df[(df['RESP'] != -1) & (df['RESP'] != 0)]['RESP'].value_counts()
    most_frequent_refusal_code = response_distribution.idxmax()
    most_frequent_refusal_count = response_distribution.max()
    # Convert 'UDATE' to string in the format 'YYYYMMDD'
    df['UDATE'] = pd.to_datetime(df['UDATE'], errors='coerce').dt.strftime('%Y%m%d')

    # Convert 'TIME' from Timedelta to string format 'HHMMSS'
    df['TIME'] = df['TIME'].apply(lambda x: f"{x.components.hours:02}{x.components.minutes:02}{x.components.seconds:02}" if pd.notnull(x) else '000000')

    # Concatenate and parse to datetime
    first_date_time = pd.to_datetime(df['UDATE'] + df['TIME'], format='%Y%m%d%H%M%S', errors='coerce').min()
    # Format the datetime
    if pd.notnull(first_date_time):
        formatted_datetime = first_date_time.strftime('%Y-%m-%d %H:%M:%S')
    else:
        formatted_datetime = 'N/A'
    # Calculate critical code rates
    critical_codes = [802, 803, 840]
    critical_code_rates = {
        f"rate_of_code_{code}": round((df['RESP'] == code).sum() / total_transactions * 100, 2)
        for code in critical_codes
    }

 
    return {
        "total_transactions": int(total_transactions),
        "successful_transactions": int(successful_transactions),
        "success_rate": round(success_rate, 2),
        "refused_transactions": int(refused_transactions),
        "refusal_rate": round(refusal_rate, 2),
        "most_frequent_refusal_code": int(most_frequent_refusal_code),
        "most_frequent_refusal_count": int(most_frequent_refusal_count),
        "latest_update": formatted_datetime, # Added field
        **critical_code_rates  # Add critical code rates dynamically


    }

@app.get("/kpis_hist/")
def get_kpis(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
   
    token = authorization.split(" ")[1]
    verify_token(token)
 
    df = load_data1()
 
    total_transactions = len(df)
    successful_transactions = df['SUCCESS'].sum()
    success_rate = (successful_transactions / total_transactions) * 100
    refused_transactions = len(df[df['RESP'] != -1])
    refusal_rate = (refused_transactions / total_transactions) * 100

     # Convert 'UDATE' to string in the format 'YYYYMMDD'
    df['UDATE'] = pd.to_datetime(df['UDATE'], errors='coerce').dt.strftime('%Y%m%d')

    # Convert 'TIME' from Timedelta to string format 'HHMMSS'
    df['TIME'] = df['TIME'].apply(lambda x: f"{x.components.hours:02}{x.components.minutes:02}{x.components.seconds:02}" if pd.notnull(x) else '000000')

    # Concatenate and parse to datetime
    first_date_time = pd.to_datetime(df['UDATE'] + df['TIME'], format='%Y%m%d%H%M%S', errors='coerce').min()
    # Format the datetime
    if pd.notnull(first_date_time):
        formatted_datetime = first_date_time.strftime('%Y-%m-%d %H:%M:%S')
    else:
        formatted_datetime = 'N/A'

 
    return {
        "total_transactions": int(total_transactions),
        "successful_transactions": int(successful_transactions),
        "success_rate": round(success_rate, 2),
        "refused_transactions": int(refused_transactions),
        "refusal_rate": round(refusal_rate, 2),
        "latest_update": formatted_datetime
    }
 
 
# Protected route: Get terminal distribution
@app.get("/terminal_distribution/")
def get_terminal_distribution(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
   
    token = authorization.split(" ")[1]
    verify_token(token)
 
    df = load_data()
    terminal_distribution = df['TERMINAL_TYPE'].value_counts().to_dict()
     # Convert 'UDATE' to string in the format 'YYYYMMDD'
    df['UDATE'] = pd.to_datetime(df['UDATE'], errors='coerce').dt.strftime('%Y%m%d')

    # Convert 'TIME' from Timedelta to string format 'HHMMSS'
    df['TIME'] = df['TIME'].apply(lambda x: f"{x.components.hours:02}{x.components.minutes:02}{x.components.seconds:02}" if pd.notnull(x) else '000000')

    # Concatenate and parse to datetime
    first_date_time = pd.to_datetime(df['UDATE'] + df['TIME'], format='%Y%m%d%H%M%S', errors='coerce').min()
    # Format the datetime
    if pd.notnull(first_date_time):
        formatted_datetime = first_date_time.strftime('%Y-%m-%d %H:%M:%S')
    else:
        formatted_datetime = 'N/A'

    return {"terminal_distribution": terminal_distribution,
            "latest_update": formatted_datetime}
 
# Protected route: Get refusal rate per issuer
@app.get("/refusal_rate_per_issuer/")
def get_refusal_rate_per_issuer(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
   
    token = authorization.split(" ")[1]
    verify_token(token)
 
    df = load_data()
    total_per_issuer = df['ISS_INST'].value_counts()
    refused_per_issuer = df[(df['RESP'] != -1) & (df['RESP'] != 0)]['ISS_INST'].value_counts()

    # Calculer le taux de refus
    refusal_rate_per_issuer = (refused_per_issuer / total_per_issuer * 100).fillna(0).round(2).to_dict()

    # Convertir les clés en entiers
    refusal_rate_per_issuer = {int(float(key)): value for key, value in refusal_rate_per_issuer.items()}

    return {"refusal_rate_per_issuer": refusal_rate_per_issuer}

 
# Protected route: Check system status
@app.get("/system_status/")
def get_system_status(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
   
    token = authorization.split(" ")[1]
    verify_token(token)
 
    df = load_data()
    df = df.sort_values(by='DATETIME')
    df['TIME_DIFF'] = df['DATETIME'].diff().dt.total_seconds()
    problematic_intervals = df[(df['TIME_DIFF'] == 30) | (df['TIME_DIFF'] == 60)]
 
    status = "Il y a un problème dans le système." if not problematic_intervals.empty else "Le système est disponible."
    return {"system_status": status}

@app.get("/transaction_trends/")
def get_transaction_trends(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    
    token = authorization.split(" ")[1]
    verify_token(token)
    
    # Load and process data
    df = load_data()
    
    # Clean the TIME column to remove the "0 days" part
    df['TIME'] = df['TIME'].astype(str).str.replace("0 days ", "", regex=False)
    
    # Convert UDATE and TIME to a combined DATETIME column
    df['DATETIME'] = pd.to_datetime(
        df['UDATE'].astype(str) + ' ' + df['TIME'],
        format='%Y-%m-%d %H:%M:%S',  # Adjusted format to match the cleaned data
        errors='coerce'
    )
    
    # Check for and drop rows with invalid DATETIME values
    invalid_rows = df[df['DATETIME'].isnull()]
    if not invalid_rows.empty:
        print("Rows with invalid datetime values after cleaning:", invalid_rows)
        df = df.dropna(subset=['DATETIME'])
    
    if df['DATETIME'].isnull().any():
        raise ValueError("Some rows have invalid datetime values after handling.")
    
    # Extract time component for grouping
    df['TIME_ONLY'] = df['DATETIME'].dt.strftime('%H:%M')

    # Group data by time and calculate success and refusal rates
    time_success_rate = df.groupby('TIME_ONLY').agg(
        total_transactions=('SUCCESS', 'size'),
        successful_transactions=('SUCCESS', 'sum'),
        refused_transactions=('SUCCESS', lambda x: x.size - x.sum())
    )
    time_success_rate['success_rate'] = (time_success_rate['successful_transactions'] / time_success_rate['total_transactions']) * 100
    time_success_rate['refusal_rate'] = (time_success_rate['refused_transactions'] / time_success_rate['total_transactions']) * 100
    time_success_rate = time_success_rate.reset_index()
    
    # Convert DataFrame to list of dictionaries for JSON response
    response_data = time_success_rate.to_dict(orient="records")

    return response_data

@app.get("/transaction_trends_hist/")
def get_transaction_trends(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    
    token = authorization.split(" ")[1]
    verify_token(token)
    
    # Load and process data
    df = load_data1()
    
    # Clean the TIME column to remove the "0 days" part
    df['TIME'] = df['TIME'].astype(str).str.replace("0 days ", "", regex=False)
    
    # Convert UDATE and TIME to a combined DATETIME column
    df['DATETIME'] = pd.to_datetime(
        df['UDATE'].astype(str) + ' ' + df['TIME'],
        format='%Y-%m-%d %H:%M:%S',  # Adjusted format to match the cleaned data
        errors='coerce'
    )
    
    # Check for and drop rows with invalid DATETIME values
    invalid_rows = df[df['DATETIME'].isnull()]
    if not invalid_rows.empty:
        print("Rows with invalid datetime values after cleaning:", invalid_rows)
        df = df.dropna(subset=['DATETIME'])
    
    if df['DATETIME'].isnull().any():
        raise ValueError("Some rows have invalid datetime values after handling.")
    
    # Extract time component for grouping
    df['TIME_ONLY'] = df['DATETIME'].dt.strftime('%H:%M')

    # Group data by time and calculate success and refusal rates
    time_success_rate = df.groupby('TIME_ONLY').agg(
        total_transactions=('SUCCESS', 'size'),
        successful_transactions=('SUCCESS', 'sum'),
        refused_transactions=('SUCCESS', lambda x: x.size - x.sum())
    )
    time_success_rate['success_rate'] = (time_success_rate['successful_transactions'] / time_success_rate['total_transactions']) * 100
    time_success_rate['refusal_rate'] = (time_success_rate['refused_transactions'] / time_success_rate['total_transactions']) * 100
    time_success_rate = time_success_rate.reset_index()
    
    # Convert DataFrame to list of dictionaries for JSON response
    response_data = time_success_rate.to_dict(orient="records")

    return response_data
# Connection manager pour WebSocket
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

@app.websocket("/ws/notifications")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            print(f"Message reçu : {data}")
            # Diffusez le message reçu à tous les clients connectés
            await manager.broadcast(data)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("Connexion WebSocket fermée.")


# Run the FastAPI server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000,ws_max_size=None)
    # Ensure the event loop runs the scheduler
    asyncio.run(schedule_async_job())
    #uvicorn.run(app, host="localhost", port=8000)


