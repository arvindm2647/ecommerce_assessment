from flask import Flask
from flask_cors import CORS
from app.database import init_db_pool

def create_app():
    app = Flask(__name__)
    CORS(app)
    
    init_db_pool()
    
    from app.routes import api_bp
    app.register_blueprint(api_bp)
    
    return app