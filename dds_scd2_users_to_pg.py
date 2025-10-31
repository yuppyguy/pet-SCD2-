import datetime
import logging
import time

from sqlalchemy import create_engine, text

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
# Параметры подключения к БД
db_url = "postgresql://postgres:postgres@localhost:5430/postgres"
engine = create_engine(db_url)

logger.info("Запуск процесса синхронизации SCD2")

while True:
    # Получаем текущее время и время минуту назад
    end_date = datetime.datetime.now(tz=datetime.UTC)
    start_date = end_date - datetime.timedelta(minutes=1)

    logger.info(f"Обработка данных за период: {start_date.isoformat()} - {end_date.isoformat()}")

    with engine.connect() as connection:  # noqa: SIM117
        # Начинаем транзакцию
        with connection.begin():
            # Проверяем, есть ли данные в целевой таблице
            result = connection.execute(text("SELECT COUNT(*) as count FROM dds.dim_scd2_users"))
            count = result.scalar()

            if count == 0:
                logger.info("Таблица dds.dim_scd2_users пуста. Выполняется начальная загрузка всех данных")

                # Подсчитываем количество записей в источнике для логирования
                result = connection.execute(text("SELECT COUNT(*) FROM raw.raw_users"))
                source_count = result.scalar()
                logger.info(f"Найдено {source_count} записей в источнике для начальной загрузки")

                # Если таблица пуста, заполняем её начальными данными в формате SCD2
                result = connection.execute(text("""
                    INSERT INTO dds.dim_scd2_users (
                        id, created_at, updated_at, first_name, last_name, middle_name,
                        birthday, email, actual_from, actual_to, ts_db
                    )
                    WITH source_data AS (
                        SELECT
                            id, created_at, updated_at, first_name, last_name, middle_name,
                            birthday, email, ts_db,
                            LEAD(ts_db) OVER (PARTITION BY id ORDER BY ts_db) as next_ts_db
                        FROM raw.raw_users
                    )
                    SELECT
                        id, created_at, updated_at, first_name, last_name, middle_name,
                        birthday, email, ts_db as actual_from, next_ts_db as actual_to, ts_db
                    FROM source_data
                """))

                # Получаем количество добавленных строк и уникальных пользователей
                result = connection.execute(text("""
                    SELECT COUNT(*) as rows, COUNT(DISTINCT id) as users FROM dds.dim_scd2_users
                """))
                stats = result.mappings().one()
                logger.info(f"Начальная загрузка завершена: добавлено {stats['rows']} записей для "
                            f"{stats['users']} уникальных пользователей")
            else:
                logger.info(f"В таблице dds.dim_scd2_users уже есть {count} записей. "
                            f"Выполняется инкрементальное обновление")

                # Проверяем наличие новых данных за указанный период
                result = connection.execute(text(f"""
                    SELECT COUNT(*) FROM raw.raw_users
                    WHERE ts_db BETWEEN '{start_date}' AND '{end_date}'
                """))
                new_data_count = result.scalar()

                if new_data_count == 0:
                    logger.info("Новых данных за указанный период не обнаружено")
                    continue

                logger.info(f"Найдено {new_data_count} новых записей за период")

                # 1. Закрываем текущие записи, которые изменились
                logger.info("Этап 1: Закрытие текущих записей, которые изменились")
                result = connection.execute(text(f"""
                    WITH latest_records AS (
                        -- Получаем последние актуальные записи для каждого id
                        SELECT id, first_name, last_name, middle_name, birthday, email
                        FROM dds.dim_scd2_users
                        WHERE actual_to IS NULL
                    ),
                    new_data AS (
                        -- Получаем новые данные за последнюю минуту
                        SELECT
                            r.id, r.created_at, r.updated_at, r.first_name, r.last_name,
                            r.middle_name, r.birthday, r.email, r.ts_db
                        FROM raw.raw_users r
                        WHERE r.ts_db BETWEEN '{start_date}' AND '{end_date}'
                    ),
                    records_to_update AS (
                        -- Находим записи, которые нужно закрыть (изменились)
                        SELECT
                            d.id
                        FROM latest_records d
                        JOIN new_data n ON d.id = n.id
                        WHERE d.first_name IS DISTINCT FROM n.first_name
                           OR d.last_name IS DISTINCT FROM n.last_name
                           OR d.middle_name IS DISTINCT FROM n.middle_name
                           OR d.birthday IS DISTINCT FROM n.birthday
                           OR d.email IS DISTINCT FROM n.email
                    )
                    -- Закрываем текущие записи
                    UPDATE dds.dim_scd2_users dst
                    SET actual_to = src.ts_db
                    FROM (
                        SELECT ru.id, ru.ts_db
                        FROM raw.raw_users ru
                        JOIN records_to_update rtu ON ru.id = rtu.id
                        WHERE ru.ts_db BETWEEN '{start_date}' AND '{end_date}'
                    ) src
                    WHERE dst.id = src.id AND dst.actual_to IS NULL
                """))
                updated_count = result.rowcount
                logger.info(f"Закрыто {updated_count} актуальных записей")

                # 2. Добавляем новые версии записей
                logger.info("Этап 2: Добавление новых версий для существующих записей")
                result = connection.execute(text(f"""
                    INSERT INTO dds.dim_scd2_users (
                        id, created_at, updated_at, first_name, last_name, middle_name,
                        birthday, email, actual_from, actual_to, ts_db
                    )
                    SELECT
                        r.id, r.created_at, r.updated_at, r.first_name, r.last_name,
                        r.middle_name, r.birthday, r.email, r.ts_db, NULL, r.ts_db
                    FROM raw.raw_users r
                    WHERE r.ts_db BETWEEN '{start_date}' AND '{end_date}'
                    AND EXISTS (
                        SELECT 1
                        FROM dds.dim_scd2_users d
                        WHERE d.id = r.id AND d.actual_to = r.ts_db
                    )
                """))
                new_versions_count = result.rowcount
                logger.info(f"Добавлено {new_versions_count} новых версий для существующих записей")

                # 3. Добавляем новые записи (которых ранее не было)
                logger.info("Этап 3: Добавление новых записей (для новых пользователей)")
                result = connection.execute(text(f"""
                    INSERT INTO dds.dim_scd2_users (
                        id, created_at, updated_at, first_name, last_name, middle_name,
                        birthday, email, actual_from, actual_to, ts_db
                    )
                    SELECT
                        r.id, r.created_at, r.updated_at, r.first_name, r.last_name,
                        r.middle_name, r.birthday, r.email, r.ts_db, NULL, r.ts_db
                    FROM raw.raw_users r
                    WHERE r.ts_db BETWEEN '{start_date}' AND '{end_date}'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM dds.dim_scd2_users d
                        WHERE d.id = r.id
                    )
                """))
                new_users_count = result.rowcount
                logger.info(f"Добавлено {new_users_count} записей для новых пользователей")

                # Итоговая статистика
                total_processed = new_versions_count + new_users_count
                logger.info(f"Итого обработано: {updated_count} закрыто, {total_processed} добавлено")

    # Ждем минуту перед следующим запуском
    now = datetime.datetime.now(tz=datetime.UTC)
    next_run = (now.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1))
    wait_seconds = (next_run - now).total_seconds() + 1  # +1 для гарантии перехода на следующую минуту
    logger.info(f"Ожидание {wait_seconds:.1f} секунд до следующего обновления ({next_run.strftime('%H:%M:%S')})")
    time.sleep(wait_seconds)