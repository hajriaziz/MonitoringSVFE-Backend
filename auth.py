from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
import bcrypt
from jwt_utils import create_access_token
import pyodbc
import logging

router = APIRouter()
# 🔹 Configuration du logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Configuration base de données
def get_db_connection():
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=172.17.235.170;'
            'DATABASE=SMT_MONITORING;'
            'UID=SMT_SVMRLogin;'
            'PWD=Monetique2026*;'
        )
        return conn
    except pyodbc.DatabaseError as e:
        print(f"Erreur connexion base de données: {e}")
        raise HTTPException(status_code=500, detail="Erreur connexion base de données")

class User(BaseModel):
    email: str
    password: str

# Hachage du mot de passe
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

# Vérification du mot de passe
def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))

# Ajout d'utilisateur
def add_user_to_db(email: str, password: str):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        hashed_password = hash_password(password)
        insert_query = "INSERT INTO Users (email, password_hash) VALUES (?, ?)"
        cursor.execute(insert_query, (email, hashed_password))
        conn.commit()
    except pyodbc.Error as e:
        print(f"Erreur ajout utilisateur: {e}")
        conn.rollback()
        raise HTTPException(status_code=500, detail="Erreur lors de l'ajout")
    finally:
        cursor.close()
        conn.close()

# Récupération d'utilisateur
def get_user_from_db(email: str):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        select_query = "SELECT * FROM Users WHERE email = ?"
        cursor.execute(select_query, (email,))
        row = cursor.fetchone()
        if row:
            columns = [column[0] for column in cursor.description]
            return dict(zip(columns, row))
        return None
    finally:
        cursor.close()
        conn.close()

# Route Inscription
@router.post("/signup/")
def sign_up(user: User):
    existing_user = get_user_from_db(user.email)
    if existing_user:
        raise HTTPException(status_code=400, detail="Cet email existe déjà.")
    add_user_to_db(user.email, user.password)
    return {"msg": "Utilisateur créé avec succès"}

# Route Connexion
@router.post("/signin/")
def sign_in(user: User):
    db_user = get_user_from_db(user.email)
    
    if db_user and verify_password(user.password, db_user["password_hash"]):
        token = create_access_token(data={"sub": user.email})
        
        # 🔹 Log de la connexion réussie
        logger.info(f"✅ Connexion réussie pour l'utilisateur : {user.email}")
        
        return {"access_token": token, "token_type": "bearer"}
    
    # 🔹 Log de la tentative de connexion échouée
    logger.warning(f"❌ Tentative de connexion échouée pour : {user.email}")
    
    raise HTTPException(status_code=401, detail="Invalid email or password")
