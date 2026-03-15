import os
from . import create_app

app = create_app()

if __name__ == "__main__":
    app.run(
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "7071")),
        debug=os.getenv("FLASK_DEBUG", "0") == "1",
    )
