import base64
from fastapi import APIRouter, File, Form, HTTPException, Depends, Header, UploadFile
from typing import Optional
from fastapi.responses import JSONResponse
import mysql.connector
from pydantic import BaseModel
from auth import get_db_connection
from jwt_utils import verify_token

router = APIRouter()

# Récupérer les informations de l'utilisateur
@router.get("/user/me")
def get_user(Authorization: str = Header(...)):
    if not Authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid token format")

    token = Authorization.split(" ")[1]
    payload = verify_token(token)

    email = payload.get("sub")  # Assuming the token payload contains 'sub' as the user's email
    if not email:
        raise HTTPException(status_code=400, detail="Email not found in token")

    conn = get_db_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        select_query = "SELECT email, username, phone, image FROM user WHERE email = %s"
        cursor.execute(select_query, (email,))
        user = cursor.fetchone()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # If the user has an image, encode it to base64
        if user['image']:
            user['image'] = base64.b64encode(user['image']).decode('utf-8')  # Convert image to base64 string
        else:
            user['image'] = None  # If no image, return None

        return JSONResponse(content=user)

    except mysql.connector.Error:
        raise HTTPException(status_code=500, detail="Database error occurred")
    finally:
        cursor.close()
        conn.close()
        
class UpdateUser(BaseModel):
    username: Optional[str] = None
    phone: Optional[int] = None
    image: Optional[bytes] = None  # Vous pouvez utiliser Base64 si vous gérez des images.

# Mise à jour des informations de l'utilisateur avec un fichier image
@router.put("/update_user/me")
async def update_user_me(
    username: Optional[str] = Form(None),
    phone: Optional[int] = Form(None),
    file: Optional[UploadFile] = File(None),
    Authorization: str = Header(...),
):
    print(f"Authorization Header: {Authorization}")
    if not Authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid token format")

    token = Authorization.split(" ")[1]
    payload = verify_token(token)
    
    # Extract email from token
    email = payload.get("sub")  # Assuming the token payload contains 'sub' as the user's email
    if not email:
        raise HTTPException(status_code=401, detail="Email not found in token")

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        update_fields = []
        values = []

        if username:
            update_fields.append("username = %s")
            values.append(username)
        if phone:
            update_fields.append("phone = %s")
            values.append(phone)
        if file:
            file_content = await file.read()
            if not file_content:
                raise HTTPException(status_code=400, detail="Uploaded file is empty")
            print(f"Received file: {file.filename}, size: {len(file_content)} bytes")

        if not update_fields:
            raise HTTPException(status_code=400, detail="No data provided for update")

        update_query = f"UPDATE user SET {', '.join(update_fields)} WHERE email = %s"
        values.append(email)

        cursor.execute(update_query, tuple(values))
        conn.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="User not found or no changes made")

        return {"msg": "User updated successfully"}
    except mysql.connector.Error:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Database error occurred")
    finally:
        cursor.close()
        conn.close()
