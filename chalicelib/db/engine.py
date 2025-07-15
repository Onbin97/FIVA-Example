import os

import firebase_admin as admin
from firebase_admin import credentials
from firebase_admin import db


if not admin._apps:
    db_url = os.getenv("DB_URL")

    prod = os.getenv("SERVER_ENV") == "prod"

    cred = credentials.Certificate('chalicelib/firebase/fiva_firebase_admin.json')
    admin.initialize_app(cred, {"databaseURL": db_url % ("default" if prod else "develop")})
root_ref = db.reference()
