from __future__ import annotations

from datetime import datetime
from io import BytesIO

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from psycopg import Error as PsycopgError


bp = Blueprint("monitor", __name__)


class ValidationError(ValueError):
    pass


def _services():
    # Все основные зависимости берутся из фабрики приложения.
    repository = current_app.extensions["repository"]
    monitoring_service = current_app.extensions["monitoring_service"]
    report_service = current_app.extensions["report_service"]
    return repository, monitoring_service, report_service


def _build_common_context(*, workshop_id: int | None = None, active_page: str = "overview"):
    repository, monitoring_service, _ = _services()
    latest = monitoring_service.latest_readings(workshop_id)
    dashboard_data = monitoring_service.dashboard()
    grafana_panels = []
    for key, panel_id in current_app.config["GRAFANA_PANELS"].items():
        grafana_panels.append(
            {
                "title": {
                    "consumption": "Потребление по оборудованию",
                    "balance": "Энергобаланс",
                    "alerts": "Тревоги и аномалии",
                }[key],
                "src": (
                    f"{current_app.config['GRAFANA_BASE_URL']}/d-solo/"
                    f"{current_app.config['GRAFANA_DASHBOARD_UID']}/energomonitor"
                    f"?orgId={current_app.config['GRAFANA_ORG_ID']}&panelId={panel_id}"
                    "&theme=light"
                ),
            }
        )
    return {
        "workshops": repository.list_workshops(),
        "equipment": repository.list_equipment(),
        "categories": repository.list_categories(),
        "sensors": repository.list_sensors(),
        "points": repository.list_points(),
        "norms": repository.list_norms(),
        "employees": repository.list_employees(),
        "reports": repository.list_reports(),
        "latest_readings": latest,
        "dashboard": dashboard_data,
        "alerts": monitoring_service.alerts()[:8],
        "grafana_panels": grafana_panels,
        "current_time": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "selected_workshop_id": workshop_id,
        "active_page": active_page,
    }


def _clean_text(value: str | None) -> str:
    return str(value or "").strip()


def _require_text(value: str | None, label: str, *, min_len: int = 1, max_len: int = 255) -> str:
    cleaned = _clean_text(value)
    if len(cleaned) < min_len:
        raise ValidationError(f"Поле «{label}» обязательно для заполнения.")
    if len(cleaned) > max_len:
        raise ValidationError(f"Поле «{label}» не должно превышать {max_len} символов.")
    return cleaned


def _require_float(value: str | None, label: str, *, minimum: float | None = None, maximum: float | None = None) -> float:
    cleaned = _clean_text(value).replace(",", ".")
    try:
        number = float(cleaned)
    except ValueError as exc:
        raise ValidationError(f"Поле «{label}» должно содержать число.") from exc
    if minimum is not None and number < minimum:
        raise ValidationError(f"Поле «{label}» должно быть не меньше {minimum}.")
    if maximum is not None and number > maximum:
        raise ValidationError(f"Поле «{label}» должно быть не больше {maximum}.")
    return number


def _require_int(value: str | None, label: str) -> int:
    cleaned = _clean_text(value)
    try:
        number = int(cleaned)
    except ValueError as exc:
        raise ValidationError(f"Поле «{label}» содержит некорректный идентификатор.") from exc
    if number <= 0:
        raise ValidationError(f"Поле «{label}» должно быть положительным.")
    return number


def _require_date(value: str | None, label: str, *, allow_datetime: bool = False) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        raise ValidationError(f"Поле «{label}» обязательно для заполнения.")
    formats = ["%Y-%m-%dT%H:%M", "%Y-%m-%d"] if allow_datetime else ["%Y-%m-%d"]
    for date_format in formats:
        try:
            datetime.strptime(cleaned, date_format)
            return cleaned
        except ValueError:
            continue
    raise ValidationError(
        f"Поле «{label}» должно быть в формате {'дата и время' if allow_datetime else 'дата'}."
    )


def _validate_equipment_form(form):
    _require_text(form.get("name"), "Наименование", min_len=3, max_len=120)
    _require_text(form.get("serial_number"), "Серийный номер", min_len=3, max_len=50)
    _require_int(form.get("workshop_id"), "Цех")
    _require_date(form.get("installed_at"), "Дата ввода")
    _require_text(form.get("status"), "Статус", min_len=3, max_len=20)
    if _clean_text(form.get("passport_data")) and len(_clean_text(form.get("passport_data"))) > 500:
        raise ValidationError("Поле «Паспортные данные оборудования» не должно превышать 500 символов.")


def _validate_sensor_form(form):
    _require_text(form.get("serial_number"), "Серийный номер датчика", min_len=3, max_len=50)
    _require_text(form.get("model"), "Модель", min_len=2, max_len=80)
    _require_text(form.get("manufacturer"), "Производитель", min_len=2, max_len=80)
    _require_text(form.get("network_address"), "Сетевой адрес", min_len=3, max_len=80)
    _require_text(form.get("protocol"), "Протокол", min_len=3, max_len=20)
    _require_int(form.get("point_id"), "Точка учета")
    _require_text(form.get("status"), "Статус", min_len=3, max_len=20)


def _validate_reading_form(form):
    _require_int(form.get("sensor_id"), "Датчик")
    _require_int(form.get("equipment_id"), "Оборудование")
    _require_float(form.get("value"), "Значение", minimum=0)
    _require_date(form.get("timestamp"), "Время показания", allow_datetime=True)


def _validate_norm_form(form):
    min_value = _require_float(form.get("min_value"), "Минимальное значение", minimum=0)
    max_value = _require_float(form.get("max_value"), "Максимальное значение", minimum=0)
    critical_value = _require_float(form.get("critical_value"), "Критическое значение", minimum=0)
    if min_value > max_value:
        raise ValidationError("Минимальное значение не может быть больше максимального.")
    if critical_value < max_value:
        raise ValidationError("Критическое значение должно быть не меньше максимального.")
    _require_int(form.get("equipment_id"), "Оборудование")
    _require_text(form.get("metric"), "Наименование параметра", min_len=2, max_len=80)
    _require_text(form.get("unit"), "Единица измерения", min_len=1, max_len=20)
    _require_text(form.get("effective_from"), "Период действия", min_len=4, max_len=80)


def _validate_report_form(form):
    _require_text(form.get("report_name"), "Название отчета", min_len=4, max_len=150)
    _require_text(form.get("report_type"), "Тип отчета", min_len=3, max_len=40)
    _require_text(form.get("report_purpose"), "Назначение отчета", min_len=5, max_len=255)
    _require_int(form.get("employee_id"), "Сотрудник")
    _require_date(form.get("date_from"), "Дата начала")
    _require_date(form.get("date_to"), "Дата окончания")
    date_from = datetime.strptime(form.get("date_from"), "%Y-%m-%d")
    date_to = datetime.strptime(form.get("date_to"), "%Y-%m-%d")
    if date_from > date_to:
        raise ValidationError("Дата начала периода не может быть позже даты окончания.")


def _handle_form_error(message: str, endpoint: str):
    flash(message, "error")
    return redirect(url_for(endpoint))


def _report_payload_by_id(report_id: int):
    repository, _, report_service = _services()
    report = next((item for item in repository.list_reports() if int(item["id"]) == int(report_id)), None)
    if not report:
        raise ValidationError("Выбранный отчет не найден.")
    date_from = report.get("date_from") or report.get("generated_at")
    date_to = report.get("date_to") or report.get("generated_at")
    payload = report_service.build_report_payload(
        report["report_type"],
        report["employee_id"],
        date_from,
        date_to,
        report_name=report.get("name"),
        register=False,
    )
    payload["name"] = report["name"]
    payload["generated_at"] = report["generated_at"]
    return report, payload


@bp.route("/")
def dashboard():
    return render_template("dashboard.html", **_build_common_context(active_page="overview"))


@bp.route("/monitoring")
def monitoring_page():
    workshop_id = request.args.get("workshop_id", type=int)
    return render_template("monitoring.html", **_build_common_context(workshop_id=workshop_id, active_page="monitoring"))


@bp.route("/registry")
def registry_page():
    return render_template("registry.html", **_build_common_context(active_page="registry"))


@bp.route("/norms-page")
def norms_page():
    return render_template("norms.html", **_build_common_context(active_page="norms"))


@bp.route("/reports-page")
def reports_page():
    return render_template("reports.html", **_build_common_context(active_page="reports"))


@bp.route("/equipment", methods=["POST"])
def create_equipment():
    repository, _, _ = _services()
    try:
        _validate_equipment_form(request.form)
        repository.add_equipment(request.form)
    except ValidationError as exc:
        return _handle_form_error(str(exc), "monitor.registry_page")
    except PsycopgError:
        return _handle_form_error("Не удалось сохранить оборудование: проверьте уникальность и обязательные поля.", "monitor.registry_page")
    flash("Оборудование добавлено в каталог.", "success")
    return redirect(url_for("monitor.registry_page"))


@bp.route("/sensors", methods=["POST"])
def create_sensor():
    repository, _, _ = _services()
    try:
        _validate_sensor_form(request.form)
        repository.add_sensor(request.form)
    except ValidationError as exc:
        return _handle_form_error(str(exc), "monitor.registry_page")
    except PsycopgError:
        return _handle_form_error("Не удалось сохранить датчик: проверьте точку учета и уникальность серийного номера.", "monitor.registry_page")
    flash("Датчик добавлен.", "success")
    return redirect(url_for("monitor.registry_page"))


@bp.route("/equipment/<int:equipment_id>", methods=["POST"])
def update_equipment(equipment_id: int):
    repository, _, _ = _services()
    try:
        _validate_equipment_form(request.form)
        repository.update_equipment(equipment_id, request.form)
    except ValidationError as exc:
        return _handle_form_error(str(exc), "monitor.registry_page")
    except PsycopgError:
        return _handle_form_error("Не удалось обновить оборудование: проверьте уникальность серийного номера и обязательные поля.", "monitor.registry_page")
    flash("Карточка оборудования обновлена.", "success")
    return redirect(url_for("monitor.registry_page"))


@bp.route("/readings", methods=["POST"])
def create_reading():
    _, monitoring_service, _ = _services()
    try:
        _validate_reading_form(request.form)
        monitoring_service.create_reading(request.form)
    except ValidationError as exc:
        return _handle_form_error(str(exc), "monitor.registry_page")
    except PsycopgError:
        return _handle_form_error("Не удалось сохранить показание: проверьте связность датчика, оборудования и времени измерения.", "monitor.registry_page")
    flash("Показание зарегистрировано и провалидировано.", "success")
    return redirect(url_for("monitor.registry_page"))


@bp.route("/sensors/<int:sensor_id>", methods=["POST"])
def update_sensor(sensor_id: int):
    repository, _, _ = _services()
    try:
        _validate_sensor_form(request.form)
        repository.update_sensor(sensor_id, request.form)
    except ValidationError as exc:
        return _handle_form_error(str(exc), "monitor.registry_page")
    except PsycopgError:
        return _handle_form_error("Не удалось обновить датчик: проверьте точку учета, серийный номер и обязательные поля.", "monitor.registry_page")
    flash("Карточка датчика обновлена.", "success")
    return redirect(url_for("monitor.registry_page"))


@bp.route("/norms", methods=["POST"])
def create_norm():
    repository, _, _ = _services()
    try:
        _validate_norm_form(request.form)
        repository.add_norm(request.form)
    except ValidationError as exc:
        return _handle_form_error(str(exc), "monitor.norms_page")
    except PsycopgError:
        return _handle_form_error("Не удалось сохранить норму: проверьте оборудование и диапазоны значений.", "monitor.norms_page")
    flash("Норма добавлена в справочник.", "success")
    return redirect(url_for("monitor.norms_page"))


@bp.route("/norms/<int:norm_id>", methods=["POST"])
def update_norm(norm_id: int):
    repository, _, _ = _services()
    try:
        _validate_norm_form(request.form)
        repository.update_norm(norm_id, request.form)
    except ValidationError as exc:
        return _handle_form_error(str(exc), "monitor.norms_page")
    except PsycopgError:
        return _handle_form_error("Не удалось обновить норму: проверьте корректность атрибутов.", "monitor.norms_page")
    flash("Норма обновлена.", "success")
    return redirect(url_for("monitor.norms_page"))


@bp.route("/reports", methods=["POST"])
def create_report():
    _, _, report_service = _services()
    try:
        _validate_report_form(request.form)
    except ValidationError as exc:
        return _handle_form_error(str(exc), "monitor.reports_page")
    report_type = request.form.get("report_type", "Сменный")
    report_name = request.form.get("report_name")
    report_purpose = request.form.get("report_purpose")
    employee_id = request.form.get("employee_id", type=int)
    date_from = request.form.get("date_from")
    date_to = request.form.get("date_to")
    payload = report_service.build_report_payload(
        report_type,
        employee_id,
        date_from,
        date_to,
        report_name=report_name,
        report_purpose=report_purpose,
        register=True,
    )
    flash(f"Отчет '{payload['name']}' сформирован.", "success")
    return redirect(url_for("monitor.reports_page"))


@bp.route("/reports/export/pdf")
def export_pdf():
    repository, _, report_service = _services()
    reports = repository.list_reports()
    latest_report = reports[-1]
    payload = report_service.build_report_payload(
        latest_report["report_type"],
        latest_report["employee_id"],
        latest_report.get("date_from") or latest_report["generated_at"],
        latest_report.get("date_to") or latest_report["generated_at"],
        report_name=latest_report.get("name"),
        register=False,
    )
    pdf_bytes = report_service.export_pdf(payload)
    return send_file(
        BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name="energomonitor-report.pdf",
    )


@bp.route("/reports/<int:report_id>")
def report_detail(report_id: int):
    try:
        report_meta, report_payload = _report_payload_by_id(report_id)
    except ValidationError as exc:
        return _handle_form_error(str(exc), "monitor.reports_page")
    context = _build_common_context(active_page="reports")
    context.update({"report_meta": report_meta, "report_payload": report_payload})
    return render_template("report_detail.html", **context)


@bp.route("/reports/<int:report_id>/pdf")
def report_pdf(report_id: int):
    try:
        report_meta, report_payload = _report_payload_by_id(report_id)
    except ValidationError as exc:
        return _handle_form_error(str(exc), "monitor.reports_page")
    _, _, report_service = _services()
    pdf_bytes = report_service.export_pdf(report_payload)
    return send_file(
        BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"report-{report_meta['id']}.pdf",
    )


@bp.route("/reports/export/xls")
def export_xls():
    repository, _, report_service = _services()
    reports = repository.list_reports()
    latest_report = reports[-1]
    payload = report_service.build_report_payload(
        latest_report["report_type"],
        latest_report["employee_id"],
        latest_report.get("date_from") or latest_report["generated_at"],
        latest_report.get("date_to") or latest_report["generated_at"],
        report_name=latest_report.get("name"),
        register=False,
    )
    xml_bytes = report_service.export_excel_xml(payload)
    return Response(
        xml_bytes,
        mimetype="application/vnd.ms-excel",
        headers={"Content-Disposition": "attachment; filename=energomonitor-report.xls"},
    )


@bp.route("/reports/<int:report_id>/xls")
def report_xls(report_id: int):
    try:
        report_meta, report_payload = _report_payload_by_id(report_id)
    except ValidationError as exc:
        return _handle_form_error(str(exc), "monitor.reports_page")
    _, _, report_service = _services()
    xml_bytes = report_service.export_excel_xml(report_payload)
    return Response(
        xml_bytes,
        mimetype="application/vnd.ms-excel",
        headers={"Content-Disposition": f"attachment; filename=report-{report_meta['id']}.xls"},
    )


@bp.route("/api/dashboard")
def api_dashboard():
    _, monitoring_service, _ = _services()
    return jsonify(monitoring_service.dashboard())


@bp.route("/api/readings")
def api_readings():
    _, monitoring_service, _ = _services()
    workshop_id = request.args.get("workshop_id", type=int)
    return jsonify(monitoring_service.latest_readings(workshop_id))


@bp.route("/api/alerts")
def api_alerts():
    _, monitoring_service, _ = _services()
    return jsonify(monitoring_service.alerts())
