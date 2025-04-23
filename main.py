import asyncio
import datetime
import logging
import pyodbc
from contextlib import asynccontextmanager
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import smtplib
from typing import List
from fastapi import  FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from auth import router as auth_router
from jwt_utils import verify_token
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
import locale
from user import router as user_router
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Scheduler pour les alertes
scheduler = AsyncIOScheduler()
# Réduire les logs de APScheduler au niveau WARNING
logging.getLogger("apscheduler").setLevel(logging.WARNING)
# Define the lifespan_events function first
@asynccontextmanager
async def lifespan_events(app: FastAPI):
    # Startup code
    print("Application startup")
    asyncio.create_task(schedule_async_job()) 
    yield  # This part indicates the running of the application
    # Shutdown cod0
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
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=172.17.235.170;'  # Replace with your SQL Server name or IP
            'DATABASE=SMT_MONITORING;'
            'UID=SMT_SVMRLogin;'
            'PWD=Monetique2026*;'
        )
        return conn
    except pyodbc.DatabaseError as e:
        print(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

    

# Function to create necessary tables
def create_tables_if_not_exists():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # SQL Server-friendly CREATE TABLE statements
    create_user_table = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='user' AND xtype='U')
    CREATE TABLE user (
        id INT IDENTITY(1,1) PRIMARY KEY,
        password_hash VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL UNIQUE,
        created_at DATETIME DEFAULT GETDATE(),
        username VARCHAR(255),
        phone BIGINT,
        image VARBINARY(MAX)
    )
    """
    
    create_monitoring_table = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='transactions' AND xtype='U')
    CREATE TABLE transactions (
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
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='alerts' AND xtype='U')
    CREATE TABLE alerts (
        id INT IDENTITY(1,1) PRIMARY KEY,
        message VARCHAR(255) NOT NULL,
        created_at DATETIME DEFAULT GETDATE()
    )
    """
    
    cursor.execute(create_user_table)
    cursor.execute(create_monitoring_table)
    cursor.execute(create_alerts_table)
    
    conn.commit()
    cursor.close()
    conn.close()


# Example query to load data from 'transactions' table
query = """
    SELECT UDATE, TIME, ISS_INST, ACQ_INST, TERMINAL_TYPE, RESP, TRANSX_NUMBER
    FROM SVISTA_Monitoring
"""
query1 = """
    SELECT UDATE, TIME, ISS_INST, ACQ_INST, TERMINAL_TYPE, RESP, TRANSX_NUMBER
    FROM SVISTA_Monitoring_Hist
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
    df = pd.DataFrame([tuple(row) for row in rows], columns=[col[0] for col in cursor.description])
    cursor.close()
    conn.close()

    # Data processing steps
    df['RESP'] = pd.to_numeric(df['RESP'], errors='coerce')

    # Ensure 'TIME' is formatted as HHMMSS (e.g., '153045' for 15:30:45)
    df['TIME'] = df['TIME'].astype(str).str.zfill(6)  # Ensures consistent 6-character format

    # Convert 'TIME' to a timedelta for further processing
    df['TIME'] = pd.to_timedelta(
        df['TIME'].str[:2] + ':' + df['TIME'].str[2:4] + ':' + df['TIME'].str[4:],
        errors='coerce'
    )

    df['DATETIME'] = pd.to_datetime(
        df['UDATE'].astype(str),
        format='%Y%m%d',
        errors='coerce'
    ) + df['TIME']

    df['SUCCESS'] = df['RESP'].apply(lambda x: 1 if x in [-1, 0] else 0)
    return df

def load_data1():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query1)
    rows = cursor.fetchall()

    # Create a DataFrame from the rows
    df = pd.DataFrame([tuple(row) for row in rows], columns=[col[0] for col in cursor.description])
    cursor.close()
    conn.close()

    # Data processing steps
    df['RESP'] = pd.to_numeric(df['RESP'], errors='coerce')

    # Ensure 'TIME' is formatted as HHMMSS (e.g., '153045' for 15:30:45)
    df['TIME'] = df['TIME'].astype(str).str.zfill(6)  # Ensures consistent 6-character format

    # Convert 'TIME' to a timedelta for further processing
    df['TIME'] = pd.to_timedelta(
        df['TIME'].str[:2] + ':' + df['TIME'].str[2:4] + ':' + df['TIME'].str[4:],
        errors='coerce'
    )

    df['DATETIME'] = pd.to_datetime(
        df['UDATE'].astype(str),
        format='%Y%m%d',
        errors='coerce'
    ) + df['TIME']

    df['SUCCESS'] = df['RESP'].apply(lambda x: 1 if x in [-1, 0] else 0)
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
        cursor.execute("SELECT email FROM Users")
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

    except locale.Error as e:
        print(f"Erreur lors de la récupération des emails : {e}")


async def check_and_send_alerts():
     print("Check and send alerts is running...")
     try:
        # Charger les données pour vérifier les KPIs
         df = load_data()
    # --- Vérification de la fraîcheur des données ---
         now = datetime.now()
         formatted_last_update = None
         delay_minutes = None
         is_data_fresh = False

         try:
            # Convertir UDATE en chaîne 'YYYYMMDD'
            df['UDATE'] = pd.to_datetime(df['UDATE'], errors='coerce').dt.strftime('%Y%m%d')

            # Convertir TIME au format HHMMSS si c’est un timedelta
            df['TIME'] = df['TIME'].apply(
                lambda x: f"{x.components.hours:02}{x.components.minutes:02}{x.components.seconds:02}" 
                if pd.notnull(x) and hasattr(x, 'components') else '000000'
            )

            # Concaténer UDATE + TIME et convertir en datetime
            datetime_series = pd.to_datetime(df['UDATE'] + df['TIME'], format='%Y%m%d%H%M%S', errors='coerce')
            last_update = datetime_series.max()

            if pd.notnull(last_update):
                last_update = last_update.to_pydatetime()
                delay_minutes = round((now - last_update).total_seconds() / 60, 2)
                is_data_fresh = delay_minutes <= 15  # Seuil de 15 minutes
                formatted_last_update = last_update.strftime('%Y-%m-%d %H:%M:%S')

                # Alerte si les données ne sont pas fraîches
                if not is_data_fresh:
                    message = (
                        f"🔴 Alerte Critique : Les données ne sont pas à jour. "
                        f"Dernière mise à jour : {formatted_last_update}, délai : {delay_minutes} minutes."
                    )
                    log_alert(message)
                    # send_email_alert(f"{message}\n\nMerci de vérifier l'état du système à partir de l'application SMTMonitoring.")
         except Exception as e:
                        print("Erreur lors du traitement de la fraîcheur des données :", str(e))
                        message = "🔴 Alerte Critique : Erreur lors de la vérification de la fraîcheur des données."
                        log_alert(message)
                        # send_email_alert(f"{message}\n\nMerci de vérifier l'état du système à partir de l'application SMTMonitoring.")
 
         total_transactions = len(df)
         successful_transactions = df['SUCCESS'].sum()
         success_rate = (successful_transactions / total_transactions) * 100
         refused_transactions = len(df[df['RESP'] != -1])
         refusal_rate = (refused_transactions / total_transactions) * 100
 
         response_distribution = df[(df['RESP'] != -1) & (df['RESP'] != 0)]['RESP'].value_counts()
         most_frequent_refusal_code = response_distribution.idxmax() if not response_distribution.empty else None
         most_frequent_refusal_count = response_distribution.max() if not response_distribution.empty else 0
 
         # Vérification des alertes
         critical_response_codes = [802, 803, 801, 840]
 
         if success_rate < 70:
             message = f"🔴 Alerte Critique : Diminution significative du taux de réussite — seulement {success_rate:.2f} %."
             #await manager.broadcast(message)
             log_alert(message)
             #send_email_alert(f"{message}\n\n""Merci de vérifier l'état du système à partir de l'application SMTMonitoring.")
 
 
         if refusal_rate > 35:
             message = f"🟠 Alerte : Le taux de refus a atteint {refusal_rate:.2f}%!"
             #await manager.broadcast(message)
             log_alert(message)
             #send_email_alert(f"{message}\n\n""Merci de vérifier l'état du système à partir de l'application SMTMonitoring.")
 
         if most_frequent_refusal_code in critical_response_codes:
             message = (
                f"🔴 Alerte Critique : Le code de refus '{most_frequent_refusal_code}' est récurrent, détecté {most_frequent_refusal_count} fois."
             )
             #await manager.broadcast(message)
             log_alert(message)
             #send_email_alert(f"{message}\n\n""Merci de vérifier l'état du système à partir de l'application SMTMonitoring.")
 
             # Vérification des taux de refus par émetteur
         total_per_issuer = df['ISS_INST'].value_counts()
         refused_per_issuer = df[(df['RESP'] != -1) & (df['RESP'] != 0)]['ISS_INST'].value_counts()
 
         refusal_rate_per_issuer = (refused_per_issuer / total_per_issuer * 100).fillna(0).round(2).to_dict()
 
         # Dictionnaire pour mapper les codes des banques avec leurs noms
         '''bank_names = {
             103: "BNA", 105: "BT", 110: "STB", 9110: "STBNet", 9108: "BIAT", 125: "ZITOUNA", 
             132: "ALBARAKA", 150: "BCT", 9101: "ATB", 9104: "ABT", 9107: "AmenB", 9111: "UBCI",
             9112: "UIB", 9114: "BH", 9117: "ONP", 9120: "BTK", 9121: "STUSID", 123: "QNB", 
             9124: "BTE", 9126: "BTL", 127: "BTS", 9128: "ABC", 133: "NAIB", 147: "WIFAKB", 
             173: "TIB", 140: "ABCI", 141: "BDL", 112: "UIB", 142: "BEA", 144: "BBA", 148: "BARKAA", 
             149: "SGA", 143: "LIB", 177: "BNAlgrie", 178: "SALAMB", 9996: "AMEXGA", 9995: "VISASMSGA", 
             9944: "MCSMSGA", 9997: "VISAGA", 9990: "BCD", 9992: "MCGA", 9968: "9968", 9145: "9145", 
             198: "198",
         }'''
         bank_names = {
         103: "BNA", 105: "BT", 9110: "STB", 9108: "BIAT", 125: "ZITOUNA", 132: "ALBARAKA", 150: "BCT",
         9101: "ATB", 9104: "ABT", 9107: "AmenB", 9111: "UBCI", 9112: "UIB", 9114: "BH", 9117: "ONP", 9120: "BTK",
         9121: "STUSID", 123: "QNB", 9124: "BTE", 9126: "BTL", 127: "BTS", 9128: "ABC", 133: "NAIB", 147: "WIFAKB", 
         173: "TIB", 140: "ABCI", 112: "UIB"
        }   

         # Filtrer les banques pour ne garder que celles présentes dans le dictionnaire
         filtered_refusal_rate = {
             bank_names[int(float(key))]: value
             for key, value in refusal_rate_per_issuer.items()
             if int(float(key)) in bank_names
         }

         # Liste des banques avec un taux de refus supérieur à 70 %
         banks_below_threshold = [
             f"{bank} ({rate:.2f}%)"
             for bank, rate in filtered_refusal_rate.items() if rate > 70
         ]

         if banks_below_threshold:
             message = (
                 "🟠 Alerte : Certaines banques émettrices présentent un taux de refus supérieur à 70 % :\n"
                 + "\n".join(banks_below_threshold)
             )
             log_alert(message)
             #send_email_alert(f"{message}\n\n""Merci de vérifier l'état du système à partir de l'application SMTMonitoring.")

        # Vérification des taux de refus par canal
         transactions_by_terminal = df.groupby('TERMINAL_TYPE').agg(
                total_transactions=('TERMINAL_TYPE', 'size'),
                successful_transactions=('SUCCESS', 'sum'),
                refused_transactions=('RESP', lambda x: (x != -1).sum())
            ).to_dict(orient='index')
            
            # Calcul des taux de refus par canal
         refusal_rate_by_channel = {
                terminal: round((data['refused_transactions'] / data['total_transactions']) * 100, 2)
                for terminal, data in transactions_by_terminal.items()
            }
                        # Dictionnaire pour mapper les canaux à leurs noms
         channel_names = {
            1: "DAB",
            2: "TPE",
            8: "E-Commerce"
         }
 
        # Vérification des taux de refus par canal et envoi d'alertes
         for channel, rate in refusal_rate_by_channel.items():
             if channel not in channel_names:  # Ignorer les canaux non définis
              continue

             if rate > 60:  # Seuil générique
                channel_name = channel_names[channel]
                message = f"🟠 Alerte : Le taux de refus est élevé sur le canal {channel_name} est élevé ({rate:.2f}%)!"
                log_alert(message)
                # Envoyer l'alerte par email
                #send_email_alert(f"{message}\n\nMerci de vérifier l'état du système à partir de l'application SMTMonitoring.")
        
        # Convertir les valeurs en entiers pour éviter les problèmes de type
         df['TERMINAL_TYPE'] = df['TERMINAL_TYPE'].astype(int)
        # Liste des types de terminaux attendus
         expected_terminal_types = set(channel_names.keys())

        # Types de terminaux réellement présents dans les transactions
         actual_terminal_types = set(df['TERMINAL_TYPE'].unique())

        # Déterminer les terminaux absents
         missing_terminal_types = expected_terminal_types - actual_terminal_types

        # Envoyer une alerte pour chaque type de terminal absent
         for terminal in missing_terminal_types:
             channel_name = channel_names.get(terminal, f"Type inconnu ({terminal})")
             message = f"🟠 Alerte : Aucune transaction n’a été détectée sur le canal {channel_name}!"
             log_alert(message)
             #send_email_alert(f"{message}\n\nMerci de vérifier l'état du système à partir de l'application SMTMonitoring.")
     except Exception as e:
         print(f"Erreur lors de la vérification des alertes : {e}")


# Function to schedule the async job
async def schedule_async_job():
    if not scheduler.get_jobs():
        scheduler.add_job(check_and_send_alerts, 'interval', minutes=10)
    # Start the scheduler (it runs as part of the asyncio event loop)
    scheduler.start()

def log_alert(message):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO Alerts (message) VALUES (?)", (message,))
        conn.commit()
        # Diffuser l'alerte via WebSocket
        asyncio.create_task(manager.broadcast(message))
        cursor.close()
        conn.close()
        #print(f"Broadcasting message: {message}")

    except locale.Error as e:
        print(f"Erreur lors de l'enregistrement de l'alerte : {e}")

@app.get("/alerts/")
def get_alerts(limit: int = 20, authorization: str = Header(None)):
     if not authorization:
         raise HTTPException(status_code=401, detail="Authorization header missing")
     token = authorization.split(" ")[1]
     verify_token(token)
     try:
         conn = get_db_connection()
         cursor = conn.cursor()
         query = f"SELECT TOP {limit} * FROM alerts ORDER BY created_at DESC"
         cursor.execute(query)
         columns = [column[0] for column in cursor.description]
         alerts = [dict(zip(columns, row)) for row in cursor.fetchall()] 
         cursor.close()
         conn.close() 
         return {"alerts": alerts} 
     except Exception as e:
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
    critical_codes = [802, 803, 801, 840]
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
 
    # Calcul de la distribution des terminaux
    terminal_distribution = df['TERMINAL_TYPE'].value_counts().to_dict()
 
    # Conversion de 'UDATE' et 'TIME' en datetime
    df['UDATE'] = pd.to_datetime(df['UDATE'], errors='coerce').dt.strftime('%Y%m%d')
    df['TIME'] = df['TIME'].apply(lambda x: f"{x.components.hours:02}{x.components.minutes:02}{x.components.seconds:02}" if pd.notnull(x) else '000000')
 
    first_date_time = pd.to_datetime(df['UDATE'] + df['TIME'], format='%Y%m%d%H%M%S', errors='coerce').min()
    formatted_datetime = first_date_time.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(first_date_time) else 'N/A'
 
    # Calcul des transactions autorisées et refusées par terminal
    total_transactions = len(df)
    transactions_by_terminal = df.groupby('TERMINAL_TYPE').agg(
        total_transactions=('TERMINAL_TYPE', 'size'),
        successful_transactions=('SUCCESS', 'sum'),
        refused_transactions=('RESP', lambda x: (x != -1).sum())
    ).to_dict(orient='index')
    
    # Calcul des taux de refus par canal
    refusal_rate_by_channel = {
        terminal: round((data['refused_transactions'] / data['total_transactions']) * 100, 2)
        for terminal, data in transactions_by_terminal.items()
    }
    
    return {
        "latest_update": formatted_datetime,
        "total_transactions": total_transactions,
        "transactions_by_terminal": transactions_by_terminal,
        "refusal_rate_by_channel": refusal_rate_by_channel
    }
 
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

    refusal_rate_per_issuer = (refused_per_issuer / total_per_issuer * 100).fillna(0).round(2).to_dict()

    # Dictionnaire des banques nationales
    bank_names = {
        103: "BNA", 105: "BT", 9110: "STB", 9108: "BIAT", 125: "ZITOUNA", 132: "ALBARAKA", 150: "BCT",
        9101: "ATB", 9104: "ABT", 9107: "AmenB", 9111: "UBCI", 9112: "UIB", 9114: "BH", 9117: "ONP",
        9120: "BTK", 9121: "STUSID", 123: "QNB", 9124: "BTE", 9126: "BTL", 127: "BTS", 9128: "ABC",
        133: "NAIB", 147: "WIFAKB", 173: "TIB", 140: "ABCI", 112: "UIB"
    }

    # Dictionnaire des banques étrangères
    foreign_bank_names = { 141: "BDL", 142: "BEA", 144: "BBA", 148: "BARKAA", 149: "SGA", 
                          143: "LIB", 177: "BNAlgrie", 178: "SALAMB", 9996: "AMEXGA",9995:"VISASMSGA",
                          9944: "MCSMSGA", 9997: "VISAGA", 9990: "BCD", 9992: "MCGA", 
                            9968: "9968", 9145: "9145", 198: "198",}

    # Banques nationales
    national_banks = {
        bank_names[int(float(key))]: value
        for key, value in refusal_rate_per_issuer.items()
        if int(float(key)) in bank_names
    }

    # Banques étrangères
    foreign_banks = {
        foreign_bank_names.get(int(float(key)), f"Code inconnu ({int(float(key))})"): value
        for key, value in refusal_rate_per_issuer.items()
        if int(float(key)) not in bank_names
    }

    return {
        "national_banks": national_banks,
        "foreign_banks": foreign_banks
    }



 
# Protected route: Check system status
@app.get("/system_status/")
def get_system_status(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
   
    token = authorization.split(" ")[1]
    verify_token(token)
 
    df = load_data()

    # --- Vérification de stabilité du système ---
    df = df.sort_values(by='DATETIME')
    df['TIME_DIFF'] = df['DATETIME'].diff().dt.total_seconds()
    problematic_intervals = df[(df['TIME_DIFF'] == 60) | (df['TIME_DIFF'] == 120)]
    is_stable = problematic_intervals.empty
    status_message = "Le système est acitf." if is_stable else "Le système est inactif"

    # --- Vérification de la fraîcheur des données ---
    now = datetime.now()
    formatted_last_update = None
    delay_minutes = None
    is_data_fresh = False

    try:
        # Convertir UDATE en chaîne 'YYYYMMDD'
        df['UDATE'] = pd.to_datetime(df['UDATE'], errors='coerce').dt.strftime('%Y%m%d')

        # Convertir TIME au format HHMMSS si c’est un timedelta
        df['TIME'] = df['TIME'].apply(
            lambda x: f"{x.components.hours:02}{x.components.minutes:02}{x.components.seconds:02}" 
            if pd.notnull(x) and hasattr(x, 'components') else '000000'
        )

        # Concaténer UDATE + TIME et convertir en datetime
        datetime_series = pd.to_datetime(df['UDATE'] + df['TIME'], format='%Y%m%d%H%M%S', errors='coerce')
        last_update = datetime_series.max()

        if pd.notnull(last_update):
            last_update = last_update.to_pydatetime()
            delay_minutes = round((now - last_update).total_seconds() / 60, 2)
            is_data_fresh = delay_minutes <= 15
            formatted_last_update = last_update.strftime('%Y-%m-%d %H:%M:%S')

    except Exception as e:
        print("Erreur lors du traitement de la fraîcheur des données :", str(e))
        # On garde les valeurs par défaut (None / False)

    # --- Réponse ---
    return {
        "system_status": status_message,
        "is_stable": is_stable,
        "is_data_fresh": is_data_fresh,
        "last_update": formatted_last_update,
        "delay_minutes": delay_minutes
    }

@app.get("/transaction_trends/")
def get_transaction_trends(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    
    # Extract and verify token
    token = authorization.split(" ")[1]
    verify_token(token)
    
    # Load and process data
    df = load_data()
    
    # Exemple de conversion correcte
    df['UDATE'] = pd.to_datetime(df['UDATE'], format='%Y-%m-%d', errors='coerce')  # Adapter le format si nécessaire
    df['TIME'] = pd.to_datetime(df['TIME'], format='%H:%M:%S', errors='coerce')  # Adapter le format si nécessaire

    # Vérifier les valeurs non convertibles
    if df['UDATE'].isnull().any() or df['TIME'].isnull().any():
        print("Certaines valeurs n'ont pas pu être converties en datetime")

    # Combiner les colonnes après conversion
    df['formatted_date_time'] = df['UDATE'].dt.strftime('%Y-%m-%d') + ' ' + df['TIME'].dt.strftime('%H:%M:%S')
    
    # Drop rows with invalid DATETIME
    df = df.dropna(subset=['DATETIME'])
    
    # Extract time component for grouping
    df['TIME_ONLY'] = df['DATETIME'].dt.strftime('%H:%M')
    
    # Group data by time and calculate metrics
    time_success_rate = df.groupby('TIME_ONLY').agg(
        total_transactions=('SUCCESS', 'size'),
        successful_transactions=('SUCCESS', 'sum'),
        refused_transactions=('SUCCESS', lambda x: x.size - x.sum())
    )
    time_success_rate['success_rate'] = (time_success_rate['successful_transactions'] / time_success_rate['total_transactions']) * 100
    time_success_rate['refusal_rate'] = (time_success_rate['refused_transactions'] / time_success_rate['total_transactions']) * 100
    time_success_rate = time_success_rate.reset_index()
    
    # Convert DataFrame to JSON response
    response_data = time_success_rate.to_dict(orient="records")

    return response_data


@app.get("/transaction_trends_hist/")
def get_transaction_trends(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    
    token = authorization.split(" ")[1]
    verify_token(token)
    
    # Charger les données
    df = load_data1()
    
    # Nettoyer la colonne TIME
    df['TIME'] = df['TIME'].apply(lambda x: str(x) if pd.notna(x) else '')
    df['TIME'] = df['TIME'].str.replace('0 days ', '', regex=False)
    
    # Reformater UDATE pour être au format 'YYYY-MM-DD'
    df['UDATE'] = pd.to_datetime(df['UDATE'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
    
    # Combiner UDATE et TIME pour créer DATETIME
    df['DATETIME'] = pd.to_datetime(df['UDATE'] + ' ' + df['TIME'], errors='coerce')
    
    # Supprimer les lignes avec des DATETIME invalides
    df = df.dropna(subset=['DATETIME'])

    # Extraire uniquement l'heure et la minute pour le regroupement
    df['TIME_ONLY'] = df['DATETIME'].dt.strftime('%H:%M')

    # Round to the nearest 30 minutes
    df['TIME_ONLY'] = df['DATETIME'].dt.floor('30min').dt.strftime('%H:%M')
    
    # Regrouper les données par ces intervalles de 30 minutes et calculer les taux de succès et de refus
    time_success_rate = df.groupby('TIME_ONLY').agg(
        total_transactions=('SUCCESS', 'size'),
        successful_transactions=('SUCCESS', 'sum'),
        refused_transactions=('SUCCESS', lambda x: x.size - x.sum())
    )
    
    time_success_rate['success_rate'] = (time_success_rate['successful_transactions'] / time_success_rate['total_transactions']) * 100
    time_success_rate['refusal_rate'] = (time_success_rate['refused_transactions'] / time_success_rate['total_transactions']) * 100
    time_success_rate = time_success_rate.reset_index()
    
    # Si le DataFrame est vide après traitement
    if time_success_rate.empty:
        raise HTTPException(status_code=404, detail="Aucune donnée valide trouvée pour les tendances des transactions.")
    
    # Convertir en format JSON pour la réponse
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


