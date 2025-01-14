import base64
from datetime import datetime
import os
from fastapi import APIRouter, File, Form, HTTPException, Depends, Header, UploadFile
from typing import Optional
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from auth import get_db_connection
from jwt_utils import verify_token

router = APIRouter()

class UserResponse(BaseModel):
    email: str
    username: Optional[str]
    phone: Optional[str]
    image: Optional[str]  # URL to access the image

UPLOAD_DIRECTORY = "./uploaded_images"

if not os.path.exists(UPLOAD_DIRECTORY):
    os.makedirs(UPLOAD_DIRECTORY, exist_ok=True)

# Récupérer les informations de l'utilisateur
@router.get("/user/me", response_model=UserResponse)
def get_user(Authorization: str = Header(...)):
    if not Authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid token format")

    token = Authorization.split(" ")[1]
    payload = verify_token(token)
    email = payload.get("sub")
    if not email:
        raise HTTPException(status_code=400, detail="Email not found in token")

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        select_query = "SELECT email, username, phone, image FROM Users WHERE email = ?"
        cursor.execute(select_query, (email,))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        # Map SQL row to dictionary
        user = {
            "email": row[0],
            "username": row[1],
            "phone": row[2],
            "image": row[3]
        }

        # Convert relative image path to base64
        if user['image']:
            image_path = os.path.join(".", user['image'])  # Construct full path
            try:
                with open(image_path, "rb") as image_file:
                    # Convert to base64
                    base64_image = base64.b64encode(image_file.read()).decode('utf-8')
                    user['image'] = f"data:image/png;base64,{base64_image}"  # Include MIME type
            except FileNotFoundError:
                user['image'] = None  # Handle missing files gracefully

        return user
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if conn:
            conn.close()

# Mise à jour des informations de l'utilisateur avec un fichier image
@router.put("/update_user/me")
async def update_user_me(
    username: Optional[str] = Form(None),
    phone: Optional[str] = Form(None),  # Adjusted to string
    file: Optional[UploadFile] = File(None),
    Authorization: str = Header(...),
):
    if not Authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid token format")
    token = Authorization.split(" ")[1]
    payload = verify_token(token)
    email = payload.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Email not found in token")

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        update_fields = []
        values = []

        relative_path = None

        # Process file upload
        if file:
            file_content = await file.read()
            file_extension = os.path.splitext(file.filename)[-1]
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = f"{email}_{timestamp}{file_extension}"
            file_path = os.path.join(UPLOAD_DIRECTORY, file_name)

            with open(file_path, "wb") as f:
                f.write(file_content)

            # Normalize and save the relative path
            relative_path = os.path.relpath(file_path, ".").replace("\\", "/")
            update_fields.append("image = ?")
            values.append(relative_path)
            
        # Update username and phone
        if username:
            update_fields.append("username = ?")
            values.append(username)
        if phone:
            update_fields.append("phone = ?")
            values.append(phone)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No data provided for update")

        update_query = f"UPDATE Users SET {', '.join(update_fields)} WHERE email = ?"
        values.append(email)

        cursor.execute(update_query, tuple(values))
        conn.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="User not found or no changes made")

        return {"msg": "User updated successfully", "image": relative_path if file else None}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if conn:
            conn.close()
