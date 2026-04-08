from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from itertools import count
from typing import Any

import psycopg
from psycopg.rows import dict_row


class InMemoryRepository:
    """Резервный репозиторий для локальной отладки без PostgreSQL."""

    def __init__(self) -> None:
        self._counters = {
            "workshop": count(4),
            "employee": count(4),
            "equipment": count(5),
            "point": count(5),
            "sensor": count(5),
            "reading": count(13),
            "norm": count(5),
            "report": count(3),
        }
        self._seed()

    def _seed(self) -> None:
        from datetime import timedelta

        now = datetime.now().replace(minute=0, second=0, microsecond=0)
        self.workshops = [
            {"id": 1, "name": "Механический цех", "location": "Корпус А", "status": "active"},
            {"id": 2, "name": "Компрессорный участок", "location": "Корпус Б", "status": "active"},
            {"id": 3, "name": "Освещение и вентиляция", "location": "Корпус В", "status": "warning"},
        ]
        self.employees = [
            {"id": 1, "tab_number": "E001", "name": "Иван Петров", "position": "Электрик", "workshop_id": 1},
            {"id": 2, "tab_number": "E002", "name": "Марина Соколова", "position": "Мастер цеха", "workshop_id": 1},
            {"id": 3, "tab_number": "E003", "name": "Олег Смирнов", "position": "Администратор", "workshop_id": 2},
        ]
        self.categories = [
            {"id": 1, "name": "Электроэнергия", "type": "Энергия", "description": "Базовый справочник", "unit": "кВт"},
            {"id": 2, "name": "Сжатый воздух", "type": "Ресурс", "description": "Технологический ресурс", "unit": "м3"},
        ]
        self.equipment = [
            {"id": 1, "serial_number": "MC-1001", "name": "Токарный станок 1К62", "workshop_id": 1, "status": "critical", "installed_at": "2024-09-12"},
            {"id": 2, "serial_number": "MC-1002", "name": "Фрезерный станок FSS-250", "workshop_id": 1, "status": "active", "installed_at": "2025-01-15"},
            {"id": 3, "serial_number": "CMP-2201", "name": "Компрессор Atlas 22", "workshop_id": 2, "status": "warning", "installed_at": "2024-07-05"},
            {"id": 4, "serial_number": "LGT-3301", "name": "Щит освещения L-3", "workshop_id": 3, "status": "active", "installed_at": "2025-03-19"},
        ]
        self.points = [
            {"id": 1, "name": "Ввод цеха 1", "workshop_id": 1, "equipment_id": None, "connected_at": "2025-01-01"},
            {"id": 2, "name": "Станочная линия 1", "workshop_id": 1, "equipment_id": 1, "connected_at": "2025-01-02"},
            {"id": 3, "name": "Станочная линия 2", "workshop_id": 1, "equipment_id": 2, "connected_at": "2025-01-03"},
            {"id": 4, "name": "Компрессорная", "workshop_id": 2, "equipment_id": 3, "connected_at": "2025-01-04"},
        ]
        self.sensors = [
            {"id": 1, "serial_number": "SN-7001", "model": "Mercury 236", "manufacturer": "Инкотекс", "protocol": "Modbus", "network_address": "10.0.0.11", "status": "online", "point_id": 1},
            {"id": 2, "serial_number": "SN-7002", "model": "Mercury 230", "manufacturer": "Инкотекс", "protocol": "Modbus", "network_address": "10.0.0.12", "status": "online", "point_id": 2},
            {"id": 3, "serial_number": "SN-7003", "model": "Owen PM210", "manufacturer": "ОВЕН", "protocol": "MQTT", "network_address": "10.0.0.31", "status": "warning", "point_id": 3},
            {"id": 4, "serial_number": "SN-7004", "model": "Eastron SDM630", "manufacturer": "Eastron", "protocol": "Modbus", "network_address": "10.0.0.45", "status": "offline", "point_id": 4},
        ]
        self.norms = [
            {"id": 1, "equipment_id": 1, "equipment_name": "Токарный станок 1К62", "equipment_serial": "MC-1001", "metric": "power_kw", "unit": "kW", "min_value": 5.0, "max_value": 17.5, "critical_value": 19.0, "effective_from": "2026-01-01"},
            {"id": 2, "equipment_id": 2, "equipment_name": "Фрезерный станок FSS-250", "equipment_serial": "MC-1002", "metric": "power_kw", "unit": "kW", "min_value": 4.0, "max_value": 11.0, "critical_value": 12.5, "effective_from": "2026-01-01"},
        ]
        seed_pattern = [
            (1, None, 49.5, "valid"),
            (1, None, 52.3, "warning"),
            (2, 1, 16.4, "valid"),
            (2, 1, 17.6, "warning"),
            (2, 1, 19.2, "critical"),
            (3, 2, 9.8, "valid"),
            (3, 2, 11.3, "warning"),
            (4, 3, 18.6, "valid"),
            (4, 3, 20.7, "warning"),
            (4, 3, 22.4, "critical"),
        ]
        self.readings = []
        for index in range(240):
            sensor_id, equipment_id, value, status = seed_pattern[index % len(seed_pattern)]
            point_multiplier = 1 + ((index % 6) * 0.015)
            self.readings.append(
                {
                    "id": index + 1,
                    "sensor_id": sensor_id,
                    "equipment_id": equipment_id,
                    "timestamp": (now - timedelta(hours=index * 4)).isoformat(timespec="minutes"),
                    "metric": "power_kw",
                    "value": round(value * point_multiplier, 2),
                    "unit": "kW",
                    "validation_status": status,
                }
            )
        self.reports = [
            {
                "id": 1,
                "name": "Сменный отчет",
                "generated_at": now.date().isoformat(),
                "report_type": "Сменный",
                "employee_id": 1,
                "description": "Период: 2026-04-07 - 2026-04-08\nДата начала: 2026-04-07\nДата окончания: 2026-04-08",
                "date_from": "2026-04-07",
                "date_to": "2026-04-08",
            },
        ]

    def list_workshops(self):
        return deepcopy(self.workshops)

    def list_employees(self):
        return deepcopy(self.employees)

    def list_categories(self):
        return deepcopy(self.categories)

    def list_equipment(self):
        return deepcopy(self.equipment)

    def list_points(self):
        return deepcopy(self.points)

    def list_sensors(self):
        return deepcopy(self.sensors)

    def list_readings(self):
        return deepcopy(self.readings)

    def list_norms(self):
        return deepcopy(self.norms)

    def list_reports(self):
        return deepcopy(self.reports)

    def add_equipment(self, payload):
        item = {
            "id": next(self._counters["equipment"]),
            "serial_number": payload["serial_number"],
            "name": payload["name"],
            "workshop_id": int(payload["workshop_id"]),
            "status": payload.get("status", "active"),
            "installed_at": payload.get("installed_at") or None,
        }
        self.equipment.append(item)
        return deepcopy(item)

    def update_equipment(self, equipment_id, payload):
        for index, item in enumerate(self.equipment):
            if item["id"] != int(equipment_id):
                continue
            updated = {
                **item,
                "serial_number": payload["serial_number"],
                "name": payload["name"],
                "workshop_id": int(payload["workshop_id"]),
                "status": payload.get("status", item.get("status", "active")),
                "installed_at": payload.get("installed_at") or item.get("installed_at"),
            }
            self.equipment[index] = updated
            return deepcopy(updated)
        raise KeyError(f"Оборудование {equipment_id} не найдено")

    def add_sensor(self, payload):
        item = {
            "id": next(self._counters["sensor"]),
            "serial_number": payload["serial_number"],
            "model": payload["model"],
            "manufacturer": payload["manufacturer"],
            "protocol": payload.get("protocol", "Modbus"),
            "network_address": payload["network_address"],
            "status": payload.get("status", "online"),
            "point_id": int(payload["point_id"]),
        }
        self.sensors.append(item)
        return deepcopy(item)

    def update_sensor(self, sensor_id, payload):
        for index, item in enumerate(self.sensors):
            if item["id"] != int(sensor_id):
                continue
            updated = {
                **item,
                "serial_number": payload["serial_number"],
                "model": payload["model"],
                "manufacturer": payload["manufacturer"],
                "protocol": payload.get("protocol", item.get("protocol", "Modbus")),
                "network_address": payload["network_address"],
                "status": payload.get("status", item.get("status", "online")),
                "point_id": int(payload["point_id"]),
            }
            self.sensors[index] = updated
            return deepcopy(updated)
        raise KeyError(f"Датчик {sensor_id} не найден")

    def add_reading(self, payload):
        item = {
            "id": next(self._counters["reading"]),
            "sensor_id": int(payload["sensor_id"]),
            "equipment_id": int(payload["equipment_id"]),
            "timestamp": payload["timestamp"],
            "metric": str(payload.get("metric", "")).strip(),
            "value": float(payload["value"]),
            "unit": payload.get("unit", "kW"),
            "validation_status": payload["validation_status"],
        }
        self.readings.append(item)
        return deepcopy(item)

    def add_norm(self, payload):
        equipment = next(item for item in self.equipment if item["id"] == int(payload["equipment_id"]))
        item = {
            "id": next(self._counters["norm"]),
            "equipment_id": equipment["id"],
            "equipment_name": equipment["name"],
            "equipment_serial": equipment["serial_number"],
            "metric": str(payload.get("metric", "")).strip(),
            "unit": payload.get("unit", "kW"),
            "min_value": float(payload["min_value"]),
            "max_value": float(payload["max_value"]),
            "critical_value": float(payload["critical_value"]),
            "effective_from": payload.get("effective_from", ""),
        }
        self.norms.append(item)
        return deepcopy(item)

    def update_norm(self, norm_id, payload):
        for index, norm in enumerate(self.norms):
            if norm["id"] != int(norm_id):
                continue
            equipment = next(item for item in self.equipment if item["id"] == int(payload["equipment_id"]))
            updated = {
                **norm,
                "equipment_id": equipment["id"],
                "equipment_name": equipment["name"],
                "equipment_serial": equipment["serial_number"],
                "metric": str(payload.get("metric", "")).strip(),
                "unit": payload.get("unit", "kW"),
                "min_value": float(payload["min_value"]),
                "max_value": float(payload["max_value"]),
                "critical_value": float(payload["critical_value"]),
                "effective_from": payload.get("effective_from", ""),
            }
            self.norms[index] = updated
            return deepcopy(updated)
        raise KeyError(f"Норма {norm_id} не найдена")

    def add_report(self, payload):
        item = {
            "id": next(self._counters["report"]),
            "name": payload["name"],
            "generated_at": payload["generated_at"],
            "report_type": payload["report_type"],
            "employee_id": int(payload["employee_id"]),
            "description": payload.get("description") or "",
            "date_from": payload.get("date_from"),
            "date_to": payload.get("date_to"),
        }
        self.reports.append(item)
        return deepcopy(item)


class PostgresRepository:
    """Репозиторий, приведенный к реальной схеме PostgreSQL базы KURS."""

    def __init__(self, *, host: str, port: int, dbname: str, user: str, password: str) -> None:
        self._conn_kwargs = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            "row_factory": dict_row,
            "autocommit": True,
        }

    def _connect(self):
        return psycopg.connect(**self._conn_kwargs)

    def ping(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()

    def _fetch_all(self, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return [dict(row) for row in cur.fetchall()]

    def _execute(self, query: str, params: tuple[Any, ...]) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)

    def _execute_returning(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                return dict(row) if row else None

    def _serialize_date(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return str(value)

    def _normalize_serial_key(self, value: Any) -> str:
        return str(value or "").strip().upper()

    def _normalize_equipment_status(self, value: Any) -> str:
        mapping = {
            "active": "active",
            "warning": "warning",
            "critical": "critical",
            "в работе": "active",
            "активен": "active",
            "исправен": "active",
            "остановлен": "warning",
            "на обслуживании": "warning",
            "неисправен": "critical",
            "авария": "critical",
        }
        return mapping.get(str(value or "active").strip().lower(), "active")

    def _normalize_sensor_status(self, value: Any) -> str:
        mapping = {
            "online": "online",
            "warning": "warning",
            "offline": "offline",
            "активен": "online",
            "в сети": "online",
            "не в сети": "offline",
            "отключен": "offline",
            "ошибка": "warning",
        }
        return mapping.get(str(value or "online").strip().lower(), "online")

    def _equipment_lookup(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[int, dict[str, Any]]]:
        rows = self._fetch_all(
            """
            SELECT serial_number, name, workshop_id, status, installation_date
            FROM public.equipment
            ORDER BY serial_number
            """
        )
        items: list[dict[str, Any]] = []
        by_serial: dict[str, dict[str, Any]] = {}
        by_id: dict[int, dict[str, Any]] = {}
        for index, row in enumerate(rows, start=1):
            item = {
                "id": index,
                "serial_number": row["serial_number"],
                "name": row["name"],
                "workshop_id": row["workshop_id"],
                "status": self._normalize_equipment_status(row.get("status")),
                "installed_at": self._serialize_date(row.get("installation_date")),
            }
            items.append(item)
            by_serial[self._normalize_serial_key(item["serial_number"])] = item
            by_id[item["id"]] = item
        return items, by_serial, by_id

    def _employees_lookup(self) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]], dict[str, dict[str, Any]]]:
        rows = self._fetch_all(
            'SELECT tab_number, full_name, workshop_id, "position" FROM public.employee ORDER BY tab_number'
        )
        items: list[dict[str, Any]] = []
        by_id: dict[int, dict[str, Any]] = {}
        by_tab: dict[str, dict[str, Any]] = {}
        for index, row in enumerate(rows, start=1):
            item = {
                "id": index,
                "tab_number": row["tab_number"],
                "name": row["full_name"],
                "position": row.get("position") or "Не указана",
                "workshop_id": row["workshop_id"],
            }
            items.append(item)
            by_id[item["id"]] = item
            by_tab[item["tab_number"]] = item
        return items, by_id, by_tab

    def _point_id_for_equipment(self, equipment_serial: str) -> int | None:
        row = self._execute_returning(
            """
            SELECT point_id
            FROM public.measurement_point
            WHERE UPPER(TRIM(equipment_serial)) = %s
            ORDER BY point_id
            LIMIT 1
            """,
            (self._normalize_serial_key(equipment_serial),),
        )
        return row["point_id"] if row else None

    def list_workshops(self):
        rows = self._fetch_all("SELECT workshop_id, name, location FROM public.workshop ORDER BY workshop_id")
        return [
            {
                "id": row["workshop_id"],
                "name": row["name"],
                "location": row.get("location") or "Не указано",
                "status": "active",
            }
            for row in rows
        ]

    def list_employees(self):
        employees, _, _ = self._employees_lookup()
        return employees

    def list_categories(self):
        rows = self._fetch_all(
            "SELECT category_id, name, type, description, unit FROM public.consumption_category ORDER BY category_id"
        )
        return [
            {
                "id": row["category_id"],
                "name": row["name"],
                "type": row.get("type") or "",
                "description": row.get("description") or "",
                "unit": row.get("unit") or "",
            }
            for row in rows
        ]

    def list_equipment(self):
        equipment_items, _, _ = self._equipment_lookup()
        return equipment_items

    def list_points(self):
        _, equipment_by_serial, _ = self._equipment_lookup()
        rows = self._fetch_all(
            """
            SELECT point_id, name, workshop_id, equipment_serial, connection_date
            FROM public.measurement_point
            ORDER BY point_id
            """
        )
        return [
            {
                "id": row["point_id"],
                "name": row["name"],
                "workshop_id": row["workshop_id"],
                "equipment_id": equipment_by_serial.get(self._normalize_serial_key(row.get("equipment_serial")), {}).get("id"),
                "connected_at": self._serialize_date(row.get("connection_date")),
            }
            for row in rows
        ]

    def list_sensors(self):
        rows = self._fetch_all(
            """
            SELECT sensor_id, serial_number, model, manufacturer, point_id,
                   transmission_protocol, network_address, installation_date, status
            FROM public.sensor
            ORDER BY sensor_id
            """
        )
        return [
            {
                "id": row["sensor_id"],
                "serial_number": row.get("serial_number") or f"sensor-{row['sensor_id']}",
                "model": row.get("model") or "Без модели",
                "manufacturer": row.get("manufacturer") or "Не указан",
                "protocol": row.get("transmission_protocol") or "Modbus",
                "network_address": row.get("network_address") or "Не указан",
                "status": self._normalize_sensor_status(row.get("status")),
                "point_id": row["point_id"],
                "installed_at": self._serialize_date(row.get("installation_date")),
            }
            for row in rows
        ]

    def list_norms(self):
        _, equipment_by_serial, _ = self._equipment_lookup()
        rows = self._fetch_all(
            """
            SELECT norm_id, parameter_name, unit, min_value, max_value,
                   critical_min, critical_max, period_of_action, equipment_serial
            FROM public.parameter_norm
            ORDER BY norm_id
            """
        )
        items = []
        for row in rows:
            equipment = equipment_by_serial.get(self._normalize_serial_key(row.get("equipment_serial")))
            if not equipment:
                continue
            critical_value = row.get("critical_max")
            if critical_value is None:
                critical_value = row.get("max_value")
            items.append(
                {
                    "id": row["norm_id"],
                    "equipment_id": equipment["id"],
                    "equipment_name": equipment["name"],
                    "equipment_serial": equipment["serial_number"],
                    "metric": row.get("parameter_name") or "",
                    "unit": row.get("unit") or "kW",
                    "min_value": float(row.get("min_value") or 0),
                    "max_value": float(row.get("max_value") or 0),
                    "critical_value": float(critical_value or 0),
                    "effective_from": row.get("period_of_action") or "",
                }
            )
        return items

    def list_readings(self):
        _, _, equipment_by_id = self._equipment_lookup()
        sensor_lookup = {item["id"]: item for item in self.list_sensors()}
        point_lookup = {item["id"]: item for item in self.list_points()}
        rows = self._fetch_all(
            """
            SELECT reading_id, sensor_id, measurement_time, value, unit, validation_status
            FROM public.reading
            ORDER BY measurement_time, reading_id
            """
        )
        items = []
        for row in rows:
            sensor = sensor_lookup.get(row["sensor_id"])
            point = point_lookup.get(sensor["point_id"]) if sensor else None
            equipment_id = point.get("equipment_id") if point else None
            timestamp = row["measurement_time"]
            items.append(
                {
                    "id": row["reading_id"],
                    "sensor_id": row["sensor_id"],
                    "equipment_id": equipment_id,
                    "timestamp": timestamp.isoformat(timespec="minutes") if isinstance(timestamp, datetime) else str(timestamp),
                    "metric": "power_kw",
                    "value": float(row["value"]),
                    "unit": row.get("unit") or "kW",
                    "validation_status": row.get("validation_status") or "unchecked",
                }
            )
        return items

    def list_reports(self):
        _, employees_by_id, employees_by_tab = self._employees_lookup()
        rows = self._fetch_all(
            "SELECT report_id, report_date, employee_tab_number, name, description FROM public.report ORDER BY report_date, report_id"
        )
        items = []
        for row in rows:
            employee = employees_by_tab.get(row["employee_tab_number"])
            report_name = row["name"]
            report_type = report_name.split(" отчет", 1)[0] if " отчет" in report_name else "Отчет"
            fallback_employee_id = next(iter(employees_by_id), 1) if employees_by_id else 1
            description = row.get("description") or ""
            date_from = None
            date_to = None
            for line in description.splitlines():
                if line.startswith("Дата начала:"):
                    date_from = line.split(":", 1)[1].strip()
                if line.startswith("Дата окончания:"):
                    date_to = line.split(":", 1)[1].strip()
            items.append(
                {
                    "id": row["report_id"],
                    "name": report_name,
                    "generated_at": self._serialize_date(row.get("report_date")),
                    "report_type": report_type,
                    "employee_id": employee["id"] if employee else fallback_employee_id,
                    "description": description,
                    "date_from": date_from,
                    "date_to": date_to,
                }
            )
        return items

    def add_equipment(self, payload):
        row = self._execute_returning(
            """
            INSERT INTO public.equipment (serial_number, name, passport_data, workshop_id, status, installation_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING serial_number
            """,
            (
                payload["serial_number"],
                payload["name"],
                payload.get("passport_data") or None,
                int(payload["workshop_id"]),
                payload.get("status", "active"),
                payload.get("installed_at") or None,
            ),
        )
        serial_number = row["serial_number"] if row else payload["serial_number"]
        return next(item for item in self.list_equipment() if item["serial_number"] == serial_number)

    def update_equipment(self, equipment_id, payload):
        _, _, equipment_by_id = self._equipment_lookup()
        current = equipment_by_id[int(equipment_id)]
        self._execute(
            """
            UPDATE public.equipment
            SET serial_number = %s,
                name = %s,
                passport_data = %s,
                workshop_id = %s,
                status = %s,
                installation_date = %s
            WHERE UPPER(TRIM(serial_number)) = %s
            """,
            (
                payload["serial_number"],
                payload["name"],
                payload.get("passport_data") or None,
                int(payload["workshop_id"]),
                payload.get("status", current.get("status", "active")),
                payload.get("installed_at") or None,
                self._normalize_serial_key(current["serial_number"]),
            ),
        )
        updated_serial = payload["serial_number"]
        return next(item for item in self.list_equipment() if self._normalize_serial_key(item["serial_number"]) == self._normalize_serial_key(updated_serial))

    def add_sensor(self, payload):
        row = self._execute_returning(
            """
            INSERT INTO public.sensor (
                serial_number, model, manufacturer, point_id,
                transmission_protocol, network_address, installation_date, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING sensor_id
            """,
            (
                payload["serial_number"],
                payload["model"],
                payload["manufacturer"],
                int(payload["point_id"]),
                payload.get("protocol", "Modbus"),
                payload["network_address"],
                None,
                payload.get("status", "online"),
            ),
        )
        sensor_id = row["sensor_id"] if row else None
        return next(item for item in self.list_sensors() if item["id"] == sensor_id)

    def update_sensor(self, sensor_id, payload):
        self._execute(
            """
            UPDATE public.sensor
            SET serial_number = %s,
                model = %s,
                manufacturer = %s,
                point_id = %s,
                transmission_protocol = %s,
                network_address = %s,
                status = %s
            WHERE sensor_id = %s
            """,
            (
                payload["serial_number"],
                payload["model"],
                payload["manufacturer"],
                int(payload["point_id"]),
                payload.get("protocol", "Modbus"),
                payload["network_address"],
                payload.get("status", "online"),
                int(sensor_id),
            ),
        )
        return next(item for item in self.list_sensors() if item["id"] == int(sensor_id))

    def add_reading(self, payload):
        row = self._execute_returning(
            """
            INSERT INTO public.reading (sensor_id, measurement_time, value, unit, validation_status, comment)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING reading_id
            """,
            (
                int(payload["sensor_id"]),
                payload["timestamp"],
                float(payload["value"]),
                payload.get("unit", "kW"),
                payload["validation_status"],
                str(payload.get("metric", "")).strip(),
            ),
        )
        reading_id = row["reading_id"] if row else None
        return next(item for item in reversed(self.list_readings()) if item["id"] == reading_id)

    def add_norm(self, payload):
        _, _, equipment_by_id = self._equipment_lookup()
        equipment = equipment_by_id[int(payload["equipment_id"])]
        point_id = self._point_id_for_equipment(equipment["serial_number"])
        row = self._execute_returning(
            """
            INSERT INTO public.parameter_norm (
                parameter_name, unit, min_value, max_value, critical_min,
                critical_max, period_of_action, point_id, equipment_serial
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING norm_id
            """,
            (
                str(payload.get("metric", "")).strip(),
                payload.get("unit", "kW"),
                float(payload["min_value"]),
                float(payload["max_value"]),
                None,
                float(payload["critical_value"]),
                payload.get("effective_from") or "",
                point_id,
                equipment["serial_number"],
            ),
        )
        norm_id = row["norm_id"] if row else None
        return next(item for item in self.list_norms() if item["id"] == norm_id)

    def update_norm(self, norm_id, payload):
        _, _, equipment_by_id = self._equipment_lookup()
        equipment = equipment_by_id[int(payload["equipment_id"])]
        point_id = self._point_id_for_equipment(equipment["serial_number"])

        # Норма хранится по оборудованию и, при наличии, по точке учета.
        self._execute(
            """
            UPDATE public.parameter_norm
            SET parameter_name = %s,
                unit = %s,
                min_value = %s,
                max_value = %s,
                critical_min = %s,
                critical_max = %s,
                period_of_action = %s,
                point_id = %s,
                equipment_serial = %s
            WHERE norm_id = %s
            """,
            (
                str(payload.get("metric", "")).strip(),
                payload.get("unit", "kW"),
                float(payload["min_value"]),
                float(payload["max_value"]),
                None,
                float(payload["critical_value"]),
                payload.get("effective_from") or "",
                point_id,
                equipment["serial_number"],
                int(norm_id),
            ),
        )
        return next(item for item in self.list_norms() if item["id"] == int(norm_id))

    def add_report(self, payload):
        _, employees_by_id, _ = self._employees_lookup()
        employee = employees_by_id[int(payload["employee_id"])]
        description = payload.get("description") or (
            f"Период: {payload.get('period_label', payload['generated_at'])}\n"
            f"Дата начала: {payload.get('date_from', payload['generated_at'])}\n"
            f"Дата окончания: {payload.get('date_to', payload['generated_at'])}"
        )
        row = self._execute_returning(
            """
            INSERT INTO public.report (report_date, employee_tab_number, name, description)
            VALUES (%s, %s, %s, %s)
            RETURNING report_id
            """,
            (
                payload["generated_at"],
                employee["tab_number"],
                payload["name"],
                description,
            ),
        )
        report_id = row["report_id"] if row else None
        return next(item for item in self.list_reports() if item["id"] == report_id)

