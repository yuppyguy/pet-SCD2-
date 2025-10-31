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


def sync_dim_users() -> None:
    """
    Синхронизирует таблицу ods.dim_users с актуальными данными из raw.raw_users.

    Использует staging таблицу в PostgreSQL для эффективного обновления данных.
    Вся логика выполняется на стороне базы данных с помощью SQL-запросов.

    :return: dict: Статистика выполнения операции
    """
    # Подключение к базе данных
    db_url = "postgresql://postgres:postgres@localhost:5430/postgres"
    engine = create_engine(db_url)

    start_time = time.time()
    with engine.begin() as conn:
        # Шаг 1: Создание временной таблицы с актуальными данными
        logger.info("Создание временной таблицы с актуальными данными")
        conn.execute(text("""
            DROP TABLE IF EXISTS stg.tmp_users;

            CREATE TABLE stg.tmp_users AS
            WITH latest_users AS (
                SELECT DISTINCT ON (id)
                    id,
                    created_at,
                    updated_at,
                    first_name,
                    last_name,
                    middle_name,
                    birthday::date,
                    email,
                    ts_db
                FROM raw.raw_users
                ORDER BY id, ts_db DESC  -- Используем ts_db для определения актуальности
            )
            SELECT * FROM latest_users;

            -- Добавляем индекс для ускорения операций
            CREATE INDEX idx_tmp_users_id ON stg.tmp_users (id);
        """))

        # Шаг 2: Получаем количество записей для статистики
        result = conn.execute(text("SELECT COUNT(*) FROM stg.tmp_users"))
        records_count = result.scalar()

        # Шаг 3: Выполняем MERGE (UPSERT) операцию для обновления ods.dim_users
        logger.info("Выполнение синхронизации данных")
        conn.execute(text("""
            INSERT INTO ods.dim_users (
                id, created_at, updated_at, first_name, last_name,
                middle_name, birthday, email
            )
            SELECT
                id, created_at, updated_at, first_name, last_name,
                middle_name, birthday, email
            FROM stg.tmp_users
            ON CONFLICT (id) DO UPDATE SET
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                middle_name = EXCLUDED.middle_name,
                birthday = EXCLUDED.birthday,
                email = EXCLUDED.email
            ;
        """))

        # Шаг 4: Удаляем временную таблицу
        logger.info("Очистка временных данных")
        conn.execute(text("DROP TABLE IF EXISTS stg.tmp_users"))

        execution_time = time.time() - start_time

        logger.info(f"Синхронизация завершена: обработано {records_count} записей за {execution_time:.2f} секунд")


logger.info("Запуск процесса синхронизации ODS слоя...")

while True:
    logger.info("Выполняется синхронизация...")

    sync_dim_users()

    logger.info("Ожидание минуты до следующего обновления...")
    time.sleep(60)