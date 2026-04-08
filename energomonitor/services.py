from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from io import BytesIO
from statistics import mean
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


class MonitoringService:
    def __init__(self, repository) -> None:
        self.repository = repository

    def _lookups(self):
        # Справочники собираются один раз на запрос, чтобы не дублировать проходы по данным.
        workshops = {item["id"]: item for item in self.repository.list_workshops()}
        equipment = {item["id"]: item for item in self.repository.list_equipment()}
        points = {item["id"]: item for item in self.repository.list_points()}
        sensors = {item["id"]: item for item in self.repository.list_sensors()}
        norms = {item["equipment_id"]: item for item in self.repository.list_norms()}
        employees = {item["id"]: item for item in self.repository.list_employees()}
        return workshops, equipment, points, sensors, norms, employees

    def validate_value(self, equipment_id: int, value: float) -> str:
        norms = {item["equipment_id"]: item for item in self.repository.list_norms()}
        norm = norms.get(equipment_id)
        if not norm:
            return "unchecked"
        if value >= norm["critical_value"]:
            return "critical"
        if value > norm["max_value"] or value < norm["min_value"]:
            return "warning"
        return "valid"

    def create_reading(self, payload):
        payload = dict(payload)
        payload["validation_status"] = self.validate_value(int(payload["equipment_id"]), float(payload["value"]))
        return self.repository.add_reading(payload)

    def latest_readings(self, workshop_id: int | None = None):
        workshops, equipment, points, sensors, norms, _ = self._lookups()
        grouped = {}
        for reading in sorted(self.repository.list_readings(), key=lambda item: item["timestamp"]):
            equipment_id = reading.get("equipment_id")
            if equipment_id not in equipment:
                continue
            if workshop_id and equipment[equipment_id]["workshop_id"] != workshop_id:
                continue
            grouped[equipment_id] = reading

        result = []
        for equipment_id, reading in grouped.items():
            equipment_item = equipment[equipment_id]
            workshop = workshops[equipment_item["workshop_id"]]
            sensor = sensors.get(reading["sensor_id"])
            norm = norms.get(equipment_id)
            point = points.get(sensor["point_id"]) if sensor else None
            result.append(
                {
                    **reading,
                    "equipment_name": equipment_item["name"],
                    "equipment_status": equipment_item["status"],
                    "workshop_name": workshop["name"],
                    "point_name": point["name"] if point else "Не привязана",
                    "sensor_label": sensor["model"] if sensor else "Не привязан",
                    "norm": norm,
                }
            )
        return sorted(result, key=lambda item: item["timestamp"], reverse=True)

    def alerts(self):
        alerts = []
        latest = self.latest_readings()
        sensor_by_id = {item["id"]: item for item in self.repository.list_sensors()}
        for reading in latest:
            if reading["validation_status"] in {"warning", "critical"}:
                alerts.append(
                    {
                        "level": reading["validation_status"],
                        "equipment_name": reading["equipment_name"],
                        "workshop_name": reading["workshop_name"],
                        "message": f"{reading['equipment_name']} зафиксировал {reading['value']:.1f} {reading['unit']}",
                        "timestamp": reading["timestamp"],
                    }
                )

        # Отдельно показываем проблемы связи по датчикам, даже если новых измерений нет.
        for sensor in sensor_by_id.values():
            if sensor["status"] != "online":
                alerts.append(
                    {
                        "level": "warning" if sensor["status"] == "warning" else "critical",
                        "equipment_name": sensor["model"],
                        "workshop_name": "Канал связи",
                        "message": f"Состояние датчика {sensor['serial_number']}: {sensor['status']}",
                        "timestamp": datetime.now().isoformat(timespec="minutes"),
                    }
                )
        return sorted(alerts, key=lambda item: (item["level"], item["timestamp"]), reverse=True)

    def dashboard(self):
        equipment = self.repository.list_equipment()
        sensors = self.repository.list_sensors()
        latest = self.latest_readings()
        alerts = self.alerts()
        norms = {item["equipment_id"]: item for item in self.repository.list_norms()}
        validation_counter = Counter(item["validation_status"] for item in latest)
        total_power = sum(item["value"] for item in latest)

        # Загрузка считается относительно суммы верхних допустимых границ по нормам.
        reference_power = sum(norm["max_value"] for norm in norms.values())
        utilization = round((total_power / reference_power) * 100, 1) if reference_power else 0

        workshop_totals = defaultdict(float)
        for item in latest:
            workshop_totals[item["workshop_name"]] += item["value"]

        return {
            "counts": {
                "workshops": len(self.repository.list_workshops()),
                "equipment": len(equipment),
                "sensors": len(sensors),
                "alerts": len(alerts),
            },
            "validation_counter": dict(validation_counter),
            "total_power": round(total_power, 2),
            "utilization": utilization,
            "workshop_totals": dict(workshop_totals),
            "critical_runtime_hours": self.critical_runtime_hours(),
            "balance": self.energy_balance(),
            "power_series": self.power_series(),
        }

    def critical_runtime_hours(self):
        critical = [item for item in self.repository.list_readings() if item["validation_status"] == "critical"]
        return len(critical)

    def power_series(self):
        series = defaultdict(list)
        equipment_lookup = {item["id"]: item["name"] for item in self.repository.list_equipment()}
        readings = sorted(self.repository.list_readings(), key=lambda item: item["timestamp"])
        for reading in readings:
            equipment_id = reading.get("equipment_id")
            if equipment_id not in equipment_lookup:
                continue
            series[equipment_lookup.get(equipment_id, "Без имени")].append(
                {"timestamp": reading["timestamp"], "value": reading["value"]}
            )
        return dict(series)

    def energy_balance(self):
        sensors = {item["id"]: item for item in self.repository.list_sensors()}
        points = {item["id"]: item for item in self.repository.list_points()}
        latest_by_point = {}
        incoming_markers = ("ввод", "вводной", "главный ввод", "вру", "щв")

        # Для энергобаланса берем последнее показание по каждой точке учета.
        for reading in sorted(self.repository.list_readings(), key=lambda item: item["timestamp"]):
            sensor = sensors.get(reading["sensor_id"])
            if not sensor:
                continue
            point = points.get(sensor["point_id"])
            if not point:
                continue
            latest_by_point[point["id"]] = {
                "value": float(reading["value"]),
                "point": point,
            }

        if not latest_by_point:
            return {"incoming": 0.0, "consumption": 0.0, "losses": 0.0}

        incoming = 0.0
        consumption = 0.0
        for item in latest_by_point.values():
            point = item["point"]
            point_name = str(point.get("name") or "").lower()
            if point.get("equipment_id") is None or any(marker in point_name for marker in incoming_markers):
                incoming += item["value"]
            else:
                consumption += item["value"]

        if incoming == 0 and consumption > 0:
            incoming = consumption

        losses = max(incoming - consumption, 0.0)
        return {
            "incoming": round(incoming, 2),
            "consumption": round(consumption, 2),
            "losses": round(losses, 2),
        }

    def average_consumption(self):
        readings = self.repository.list_readings()
        if not readings:
            return 0.0
        return round(mean(item["value"] for item in readings), 2)


class ReportService:
    def __init__(self, repository, monitoring_service: MonitoringService) -> None:
        self.repository = repository
        self.monitoring_service = monitoring_service
        self._font_name = self._register_pdf_font()

    def _register_pdf_font(self) -> str:
        candidates = [
            ("Arial", "C:/Windows/Fonts/arial.ttf"),
            ("TimesNewRoman", "C:/Windows/Fonts/times.ttf"),
        ]
        for font_name, font_path in candidates:
            try:
                pdfmetrics.registerFont(TTFont(font_name, font_path))
                return font_name
            except Exception:
                continue
        return "Helvetica"

    def _parse_timestamp(self, value: str) -> datetime:
        normalized = str(value).replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            parsed = datetime.strptime(str(value)[:16], "%Y-%m-%dT%H:%M")
        if parsed.tzinfo is not None:
            return parsed.replace(tzinfo=None)
        return parsed

    def _period_bounds(self, date_from: str, date_to: str) -> tuple[datetime, datetime]:
        start = datetime.strptime(date_from, "%Y-%m-%d")
        finish = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1) - timedelta(minutes=1)
        return start, finish

    def _all_enriched_readings(self):
        workshops, equipment, points, sensors, norms, _ = self.monitoring_service._lookups()
        items = []
        for reading in sorted(self.repository.list_readings(), key=lambda item: item["timestamp"], reverse=True):
            sensor = sensors.get(reading["sensor_id"])
            point = points.get(sensor["point_id"]) if sensor else None
            equipment_id = reading.get("equipment_id")
            equipment_item = equipment.get(equipment_id) if equipment_id is not None else None
            workshop_id = (
                equipment_item["workshop_id"]
                if equipment_item
                else point.get("workshop_id") if point else None
            )
            workshop = workshops.get(workshop_id, {"name": "Не указан"})
            norm = norms.get(equipment_id) if equipment_id is not None else None
            items.append(
                {
                    **reading,
                    "equipment_name": equipment_item["name"] if equipment_item else (point["name"] if point else "Ввод"),
                    "equipment_status": equipment_item["status"] if equipment_item else "active",
                    "workshop_name": workshop["name"],
                    "point_name": point["name"] if point else "Не привязана",
                    "sensor_label": sensor["model"] if sensor else "Не привязан",
                    "norm": norm,
                }
            )
        return items

    def _readings_for_period(self, date_from: str, date_to: str):
        start, finish = self._period_bounds(date_from, date_to)
        rows = []
        for item in self._all_enriched_readings():
            timestamp = self._parse_timestamp(item["timestamp"])
            if start <= timestamp <= finish:
                rows.append(item)
        return rows

    def _balance_from_rows(self, rows):
        if not rows:
            return {"incoming": 0.0, "consumption": 0.0, "losses": 0.0}

        latest_by_point = {}
        incoming_markers = ("ввод", "вводной", "главный ввод", "вру", "щв")
        for item in sorted(rows, key=lambda current: current["timestamp"]):
            latest_by_point[item["point_name"]] = item

        incoming = 0.0
        consumption = 0.0
        for item in latest_by_point.values():
            point_name = str(item["point_name"]).lower()
            if any(marker in point_name for marker in incoming_markers):
                incoming += float(item["value"])
            else:
                consumption += float(item["value"])

        if incoming == 0 and consumption > 0:
            incoming = consumption

        return {
            "incoming": round(incoming, 2),
            "consumption": round(consumption, 2),
            "losses": round(max(incoming - consumption, 0.0), 2),
        }

    def _latest_rows_by_equipment(self, rows):
        latest = {}
        for row in sorted(rows, key=lambda item: item["timestamp"], reverse=True):
            latest.setdefault(row["equipment_id"], row)
        return list(latest.values())

    def _build_period_summary(self, rows):
        if not rows:
            return {
                "total_power": 0.0,
                "average_consumption": 0.0,
                "utilization": 0.0,
                "alerts": 0,
                "critical_runtime_hours": 0,
                "incoming": 0.0,
                "consumption": 0.0,
                "losses": 0.0,
            }

        total_value = round(sum(float(item["value"]) for item in rows), 2)
        average_value = round(mean(float(item["value"]) for item in rows), 2)
        alerts_count = sum(1 for item in rows if item["validation_status"] in {"warning", "critical"})
        critical_count = sum(1 for item in rows if item["validation_status"] == "critical")

        reference_total = 0.0
        for item in rows:
            norm = item.get("norm")
            if norm and norm.get("max_value"):
                reference_total += float(norm["max_value"])
        utilization = round((total_value / reference_total) * 100, 1) if reference_total else 0.0

        balance = self._balance_from_rows(rows)

        return {
            "total_power": total_value,
            "average_consumption": average_value,
            "utilization": utilization,
            "alerts": alerts_count,
            "critical_runtime_hours": critical_count,
            "incoming": balance["incoming"],
            "consumption": balance["consumption"],
            "losses": balance["losses"],
        }

    def _build_highlights(self, report_type: str, rows, summary: dict[str, float], date_from: str, date_to: str):
        if not rows:
            return [
                "За выбранный период показания в системе не зарегистрированы.",
                "Для формирования содержательного отчета нужно выбрать период, в котором есть сохраненные измерения.",
            ]

        peak = max(rows, key=lambda item: float(item["value"]))
        critical_rows = [item for item in rows if item["validation_status"] == "critical"]
        warning_rows = [item for item in rows if item["validation_status"] == "warning"]
        affected_equipment = sorted({item["equipment_name"] for item in critical_rows + warning_rows})

        if report_type == "Сменный":
            return [
                f"Смена охватывает период {date_from} - {date_to}; в отчет включено {len(rows)} измерений.",
                f"Пиковая нагрузка зафиксирована на оборудовании «{peak['equipment_name']}» и составила {peak['value']} {peak['unit']}.",
                f"Количество отклонений за смену: {len(warning_rows)} предупреждений и {len(critical_rows)} критических событий.",
            ]
        if report_type == "Суточный":
            latest_rows = self._latest_rows_by_equipment(rows)
            return [
                f"Суточный отчет отражает состояние {len(latest_rows)} единиц оборудования по последним показаниям за сутки.",
                f"Среднее значение потребления за сутки составило {summary['average_consumption']} кВт.",
                f"Наибольшая нагрузка наблюдалась на оборудовании «{peak['equipment_name']}».",
            ]
        if report_type == "Недельный":
            daily_totals = defaultdict(float)
            for item in rows:
                daily_totals[str(item["timestamp"])[:10]] += float(item["value"])
            busiest_day = max(daily_totals.items(), key=lambda item: item[1])
            return [
                f"За неделю обработано {len(rows)} измерений по оборудованию производственного участка.",
                f"Максимальный объем нагрузки пришелся на {busiest_day[0]} и составил {round(busiest_day[1], 2)} кВт.",
                f"Отклонения фиксировались по следующим объектам: {', '.join(affected_equipment[:4]) or 'не выявлены'}.",
            ]
        return [
            f"Аналитический отчет подготовлен по периоду {date_from} - {date_to} на основании {len(rows)} показаний.",
            f"Выявлено {len(critical_rows)} критических и {len(warning_rows)} предупредительных отклонений.",
            f"Наиболее нагруженный объект периода: «{peak['equipment_name']}» ({peak['value']} {peak['unit']}).",
        ]

    def _build_report_rows(self, report_type: str, rows):
        if report_type == "Суточный":
            return sorted(self._latest_rows_by_equipment(rows), key=lambda item: item["equipment_name"])
        if report_type == "Недельный":
            return sorted(rows, key=lambda item: item["timestamp"], reverse=True)[:28]
        if report_type == "Аналитический":
            prioritized = sorted(
                rows,
                key=lambda item: (
                    {"critical": 0, "warning": 1, "valid": 2, "unchecked": 3}.get(item["validation_status"], 9),
                    -float(item["value"]),
                ),
            )
            return prioritized[:24]
        return sorted(rows, key=lambda item: item["timestamp"], reverse=True)[:24]

    def build_report_payload(
        self,
        report_type: str,
        employee_id: int,
        date_from: str,
        date_to: str,
        report_name: str | None = None,
        report_purpose: str | None = None,
        *,
        register: bool = True,
    ):
        employees = {item["id"]: item for item in self.repository.list_employees()}
        rows = self._readings_for_period(date_from, date_to)
        period_label = f"{date_from} - {date_to}"
        summary = self._build_period_summary(rows)
        detail_rows = self._build_report_rows(report_type, rows)
        final_name = str(report_name or "").strip() or f"{report_type} отчет за {period_label}"
        final_purpose = str(report_purpose or "").strip() or "Контроль энергопотребления оборудования цеха"
        payload = {
            "name": final_name,
            "generated_at": datetime.now().date().isoformat(),
            "report_type": report_type,
            "employee_id": employee_id,
            "date_from": date_from,
            "date_to": date_to,
            "period_label": period_label,
            "employee_name": employees[employee_id]["name"],
            "purpose": final_purpose,
            "summary": summary,
            "rows": detail_rows,
            "total_rows": len(rows),
            "detail_title": {
                "Сменный": "Показания за смену",
                "Суточный": "Итоговые значения за сутки",
                "Недельный": "Контрольные показания за неделю",
                "Аналитический": "Наиболее значимые показания периода",
            }.get(report_type, "Показания, включенные в отчет"),
            "highlights": self._build_highlights(report_type, rows, summary, date_from, date_to),
            "description": (
                f"Период: {period_label}\n"
                f"Дата начала: {date_from}\n"
                f"Дата окончания: {date_to}\n"
                f"Тип отчета: {report_type}\n"
                f"Назначение: {final_purpose}"
            ),
        }
        if register:
            self.repository.add_report(payload)
        return payload

    def export_pdf(self, report_payload) -> bytes:
        # PDF нужен для демонстрации отчетности без сторонних сервисов.
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, title=report_payload["name"])
        styles = getSampleStyleSheet()
        for style_name in ("Title", "Heading2", "BodyText"):
            styles[style_name].fontName = self._font_name
        story = [
            Paragraph("АС ЭнергоМонитор", styles["Title"]),
            Spacer(1, 8),
            Paragraph(report_payload["name"], styles["Heading2"]),
            Paragraph(f"Сформировал: {report_payload['employee_name']}", styles["BodyText"]),
            Paragraph(f"Дата: {report_payload['generated_at']}", styles["BodyText"]),
            Spacer(1, 12),
        ]

        summary_rows = [
            ["Показатель", "Значение"],
            ["Текущая нагрузка, кВт", str(report_payload["summary"]["total_power"])],
            ["Среднее потребление, кВт", str(report_payload["summary"]["average_consumption"])],
            ["Загрузка по нормам, %", str(report_payload["summary"]["utilization"])],
            ["Сработавшие уведомления", str(report_payload["summary"]["alerts"])],
            ["Часы в критическом режиме", str(report_payload["summary"]["critical_runtime_hours"])],
            ["Энергобаланс: приход", str(report_payload["summary"]["incoming"])],
            ["Энергобаланс: расход", str(report_payload["summary"]["consumption"])],
            ["Энергопотери", str(report_payload["summary"]["losses"])],
        ]
        summary_table = Table(summary_rows, colWidths=[220, 120])
        summary_table.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, -1), self._font_name),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f3d3e")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#f5f3ea")),
                ]
            )
        )
        story.extend([summary_table, Spacer(1, 14)])

        rows = [["Оборудование", "Цех", "Значение", "Статус", "Время"]]
        for item in report_payload["rows"][:12]:
            rows.append(
                [
                    item["equipment_name"],
                    item["workshop_name"],
                    f"{item['value']:.1f} {item['unit']}",
                    item["validation_status"],
                    item["timestamp"],
                ]
            )
        readings_table = Table(rows, colWidths=[120, 110, 75, 70, 95])
        readings_table.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, -1), self._font_name),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2f7d6b")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ]
            )
        )
        story.append(readings_table)
        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()

    def export_excel_xml(self, report_payload) -> bytes:
        header = """<?xml version="1.0"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
<Worksheet ss:Name="EnergyReport"><Table>"""
        footer = "</Table></Worksheet></Workbook>"

        rows = [
            ["Отчет", report_payload["name"]],
            ["Дата", report_payload["generated_at"]],
            ["Сформировал", report_payload["employee_name"]],
            ["Текущая нагрузка, кВт", str(report_payload["summary"]["total_power"])],
            ["Среднее потребление, кВт", str(report_payload["summary"]["average_consumption"])],
            ["Энергопотери, кВт", str(report_payload["summary"]["losses"])],
            ["", ""],
            ["Оборудование", "Цех", "Значение", "Статус", "Время"],
        ]
        for item in report_payload["rows"]:
            rows.append(
                [
                    item["equipment_name"],
                    item["workshop_name"],
                    f"{item['value']:.1f} {item['unit']}",
                    item["validation_status"],
                    item["timestamp"],
                ]
            )

        xml_rows = []
        for row in rows:
            cells = "".join(
                f'<Cell><Data ss:Type="String">{escape(str(cell))}</Data></Cell>'
                for cell in row
            )
            xml_rows.append(f"<Row>{cells}</Row>")
        return (header + "".join(xml_rows) + footer).encode("utf-8")
