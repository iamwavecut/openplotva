-- Source SHA-256: c1b60123d439042e2e556a65456102e27737482382b3e2fc93fbd4259c548f08

CREATE TABLE job_queue (
    id BIGSERIAL PRIMARY KEY,
    queue_name VARCHAR(50) NOT NULL,           -- 'text', 'image-regular', 'image-vip'
    priority INTEGER NOT NULL DEFAULT 0,       -- -4 to 4 (Lowest to Highest)
    title VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,                    -- сериализованные данные задания
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed', 'cancelled'

    -- Связь с Telegram сообщениями
    user_id BIGINT NOT NULL,                   -- ID пользователя Telegram
    chat_id BIGINT NOT NULL,                   -- ID чата Telegram
    trigger_message_id INTEGER NOT NULL,       -- ID сообщения, инициировавшего задание
    thread_message_id INTEGER,                 -- ID топика в форуме (если применимо)

    -- Активные сообщения для управления
    progress_message_id INTEGER,               -- ID сообщения с прогрессом (sticker, текст)
    queue_position_message_id INTEGER,         -- ID сообщения с позицией в очереди
    result_message_id INTEGER,                 -- ID сообщения с результатом

    -- Метаданные выполнения
    worker_id VARCHAR(100),                    -- UUID воркера, взявшего задание
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,                    -- когда воркер начал обработку
    completed_at TIMESTAMPTZ,                  -- когда задание завершено

    -- Retry логика
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    next_retry_at TIMESTAMPTZ,                 -- когда можно повторить
    original_job_id BIGINT,                    -- ссылка на исходное задание при retry

    -- Дополнительная информация
    error_message TEXT,                        -- ошибка при выполнении
    processing_timeout_seconds INTEGER DEFAULT 300, -- таймаут обработки

    -- Метаданные для оптимизации
    prompt_hash VARCHAR(64),                   -- хеш промпта для дедупликации
    estimated_processing_time INTEGER,         -- оценочное время обработки (секунды)
    actual_processing_time INTEGER,            -- фактическое время обработки (секунды)

    -- Constraints
    CONSTRAINT valid_status CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')),
    CONSTRAINT fk_original_job FOREIGN KEY (original_job_id) REFERENCES job_queue(id)
);

-- Индексы для оптимизации запросов
CREATE INDEX idx_job_queue_status_priority ON job_queue(status, priority DESC, created_at);
CREATE INDEX idx_job_queue_queue_name ON job_queue(queue_name);
CREATE INDEX idx_job_queue_worker_id ON job_queue(worker_id);
CREATE INDEX idx_job_queue_next_retry ON job_queue(next_retry_at) WHERE status = 'failed';
CREATE INDEX idx_job_queue_processing_timeout ON job_queue(started_at, processing_timeout_seconds) WHERE status = 'processing';

-- Новые индексы для оптимизации процессов рисования
CREATE INDEX idx_job_queue_user_status ON job_queue(user_id, status);
CREATE INDEX idx_job_queue_chat_user ON job_queue(chat_id, user_id, created_at DESC);
CREATE INDEX idx_job_queue_trigger_message ON job_queue(chat_id, trigger_message_id);
-- Индекс для технической дедупликации (только в пределах чата) - убираем временное ограничение из индекса
CREATE INDEX idx_job_queue_dedup ON job_queue(chat_id, user_id, prompt_hash, created_at DESC)
    WHERE status IN ('pending', 'processing');
CREATE INDEX idx_job_queue_progress_messages ON job_queue(progress_message_id) WHERE progress_message_id IS NOT NULL;

-- Таблица для детального отслеживания сообщений
CREATE TABLE job_messages (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL,
    message_type VARCHAR(20) NOT NULL,         -- 'progress', 'queue_position', 'result', 'error', 'sticker'
    chat_id BIGINT NOT NULL,
    message_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,                    -- когда сообщение должно быть удалено
    is_ephemeral BOOLEAN DEFAULT FALSE,        -- временное сообщение для удаления

    CONSTRAINT fk_job_messages_job FOREIGN KEY (job_id) REFERENCES job_queue(id) ON DELETE CASCADE,
    CONSTRAINT valid_message_type CHECK (message_type IN ('progress', 'queue_position', 'result', 'error', 'sticker'))
);

-- Индексы для быстрого поиска сообщений
CREATE INDEX idx_job_messages_job_id ON job_messages(job_id);
CREATE INDEX idx_job_messages_expires_at ON job_messages(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX idx_job_messages_ephemeral ON job_messages(is_ephemeral, expires_at) WHERE is_ephemeral = TRUE;
CREATE INDEX idx_job_messages_chat_message ON job_messages(chat_id, message_id);
