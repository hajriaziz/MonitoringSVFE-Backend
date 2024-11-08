from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import bcrypt
from jwt_utils import create_access_token
import mysql.connector
from mysql.connector import Error
from fastapi.middleware.cors import CORSMiddleware
router = APIRouter()

# Database connection function
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

class User(BaseModel):
    email: str
    password: str

# Hash password
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# Verify password
def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))

# Function to add a user to the database
def add_user_to_db(email: str, password: str):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        hashed_password = hash_password(password)
        insert_query = "INSERT INTO user (email, password_hash) VALUES (%s, %s)"
        cursor.execute(insert_query, (email, hashed_password))
        conn.commit()  # Commit the transaction
        print("User added to the database:", email)  # Debug print statement
    except Error as e:
        print("Error adding user to database:", e)
        conn.rollback()  # Rollback in case of error
        raise HTTPException(status_code=500, detail="Database error occurred")
    finally:
        cursor.close()
        conn.close()

# Function to retrieve a user from the database
def get_user_from_db(email: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    select_query = "SELECT * FROM user WHERE email = %s"
    cursor.execute(select_query, (email,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    return user

# Sign-up route
@router.post("/signup/")
def sign_up(user: User):
    # Check if user already exists in the database
    existing_user = get_user_from_db(user.email)
    if existing_user:
        raise HTTPException(status_code=400, detail="email already exists")

    # Add new user to the database
    add_user_to_db(user.email, user.password)
    return {"msg": "User created successfully"}

# Sign-in route to authenticate users and return JWT token
@router.post("/signin/")
def sign_in(user: User):
    db_user = get_user_from_db(user.email)
    if db_user and verify_password(user.password, db_user["password_hash"]):
        token = create_access_token(data={"sub": user.email})
        return {"access_token": token, "token_type": "bearer"}
    
    raise HTTPException(status_code=401, detail="Invalid email or password")
