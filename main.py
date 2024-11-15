import json
from typing import Optional
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.security import OAuth2PasswordBearer
import jwt
from pydantic import BaseModel
from auth import router as auth_router
from jwt_utils import ALGORITHM, SECRET_KEY, verify_token
import pandas as pd
import mysql.connector
from mysql.connector import Error
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime 
import locale
from user import router as user_router

# Create the main FastAPI app
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can specify allowed origins, e.g., ["http://localhost:3000"]
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
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
    critical_codes = [802, 803, 910]
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
    refusal_rate_per_issuer = (refused_per_issuer / total_per_issuer * 100).fillna(0).round(2).to_dict()
 
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

# Run the FastAPI server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    #uvicorn.run(app, host="localhost", port=8000)


