from pathlib import Path

from flask import Flask

from .repository import InMemoryRepository, PostgresRepository
from .services import MonitoringService, ReportService


BASE_DIR = Path(__file__).resolve().parent.parent


def create_app() -> Flask:
    # Фабрика приложения собирает конфигурацию, репозиторий и сервисный слой.
    app = Flask(
        __name__,
        template_folder=str(BASE_DIR / "templates"),
        static_folder=str(BASE_DIR / "static"),
    )
    app.config.update(
        SECRET_KEY="energomonitor-dev",
        GRAFANA_BASE_URL="http://localhost:3000",
        GRAFANA_DASHBOARD_UID="adqx7zr",
        GRAFANA_ORG_ID=1,
        GRAFANA_PANELS={
            "consumption": 1,
            "balance": 2,
            "alerts": 3,
        },
        POSTGRES_HOST="localhost",
        POSTGRES_PORT=5432,
        POSTGRES_DB="KURS1",
        POSTGRES_USER="postgres",
        POSTGRES_PASSWORD="admin",
        USE_INMEMORY_REPOSITORY=False,
    )

    if app.config["USE_INMEMORY_REPOSITORY"]:
        repository = InMemoryRepository()
    else:
        # В рабочем режиме приложение использует PostgreSQL из схемы KURS.
        repository = PostgresRepository(
            host=app.config["POSTGRES_HOST"],
            port=app.config["POSTGRES_PORT"],
            dbname=app.config["POSTGRES_DB"],
            user=app.config["POSTGRES_USER"],
            password=app.config["POSTGRES_PASSWORD"],
        )
        repository.ping()

    monitoring_service = MonitoringService(repository)
    report_service = ReportService(repository, monitoring_service)

    # Сервисы сохраняются в extensions, чтобы их можно было получать в маршрутах.
    app.extensions["repository"] = repository
    app.extensions["monitoring_service"] = monitoring_service
    app.extensions["report_service"] = report_service

    from .views import bp

    app.register_blueprint(bp)
    return app
